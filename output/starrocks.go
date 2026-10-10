package output

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-lazyer/north/nhttp"
	"github.com/go-lazyer/nsync/config"
	"github.com/go-lazyer/nsync/input"
)

type StarrocksOutput struct {
	auth       string
	buffers    []*input.Event //mysql
	bufferTime time.Time      // 每个表的批次时间
	conf       *config.OutputConfig
	mu         sync.Mutex //互斥锁
	batchSize  int
}

func (output *StarrocksOutput) NewOutput(cfg *config.OutputConfig) error {
	output.conf = cfg
	s := cfg.Starrocks.Username + ":" + cfg.Starrocks.Password
	b := []byte(s)
	output.auth = base64.StdEncoding.EncodeToString(b)
	// 初始化缓冲区
	output.buffers = make([]*input.Event, 0)
	// 初始化缓冲区时间
	output.bufferTime = time.Now()
	output.batchSize = cfg.Starrocks.BatchSize
	if output.batchSize <= 0 {
		output.batchSize = DefaultMaxBatchSize
	}
	return nil
}

// transactionHeaders 生成事务请求公共头，默认开启 transaction_type=multi。
func (output *StarrocksOutput) transactionHeaders(label string) map[string]string {
	headers := map[string]string{
		"Authorization":    "Basic " + output.auth,
		"Expect":           "100-continue",
		"label":            label,
		"db":               output.conf.Starrocks.Database,
		"transaction_type": "multi",
		"load_timeout":     "300", //测试使用 3600 秒，生产环境可调整或改为配置项
	}

	return headers
}

// isStatusOK 判断 StarRocks 返回状态是否可视为成功。
func (output *StarrocksOutput) isStatusOK(status string, message string) bool {
	if strings.EqualFold(status, "OK") || strings.EqualFold(status, "Success") {
		return true
	}
	return false
}

// parseTransactionResult 将事务接口响应统一转换为错误语义。
func (output *StarrocksOutput) parseTransactionResult(op string, result map[string]interface{}) error {
	status, _ := result["Status"].(string)
	message, _ := result["Message"].(string)
	if output.isStatusOK(status, message) {
		return nil
	}
	if message == "" {
		message = fmt.Sprintf("status=%s", status)
	}
	return fmt.Errorf("starrocks %s failed: %s", op, message)
}

// beginTransaction 开启一个事务，始终按 multi 事务模式请求。
func (output *StarrocksOutput) beginTransaction(label string, table string) error {
	headers := output.transactionHeaders(label)
	// headers["table"] = table
	host := output.conf.Starrocks.FeHost
	port := output.conf.Starrocks.FeHttpPort
	loadURL := fmt.Sprintf("http://%s:%d/api/transaction/begin", host, port)

	req := nhttp.NewRequest()

	req.CheckRedirect(func(req *http.Request, via []*http.Request) error {

		if "http://182.92.121.152:8030/api/transaction/begin" != req.Referer() {
			return nil
		}
		println("重定向URL:", req.URL.String())
		u := req.URL.String()
		u = strings.ReplaceAll(u, "172.17.205.153", "182.92.121.152")

		newUrl, _ := url.Parse(u)
		req.URL = newUrl
		// Go 重定向到不同 host 时会自动剥离 Authorization，
		// 但 CheckRedirect 在剥离之后、发送之前执行，手动补回即可
		req.Header.Set("Authorization", headers["Authorization"])
		req.Header.Set("Expect", "100-continue")
		return nil
	})

	response, err := req.Header(headers).Timeout(60).Do("POST", loadURL, nil)
	if err != nil {
		return err
	}
	result, err := parseResponse(response)
	if err != nil {
		return err
	}
	return output.parseTransactionResult("begin", result)
}

// loadTransaction 向事务中写入单行 JSON 数据，并区分 upsert / delete / partial update。
func (output *StarrocksOutput) loadTransaction(label string, event *input.Event) error {
	if event == nil {
		return fmt.Errorf("event cannot be null")
	}

	if event.Table == "" {
		return fmt.Errorf("starrocks load failed: empty table")
	}

	var row map[string]interface{}
	headers := output.transactionHeaders(label)
	headers["table"] = event.Table
	headers["format"] = "json"
	headers["strip_outer_array"] = "true"
	// headers["columns"] = "__op"
	switch event.Action {

	case "insert":
		if len(event.AfterRow) == 0 {
			return fmt.Errorf("starrocks load failed: action is insert event cannot be empty")
		}
		row = event.ToAfterMap()
		// return nil
	case "update":
		if len(event.AfterRow) == 0 {
			return fmt.Errorf("starrocks load failed: action is update event cannot be empty")
		}
		row = event.ToAfterMap()
		// // update 语义按 partial update 发送。
		// headers["partial_update"] = "true"
		// headers["columns"] = strings.Join(event.Columns, ",")
		// return nil
	case "replace":
		if len(event.AfterRow) == 0 {
			return fmt.Errorf("starrocks load failed: action is replace event cannot be empty")
		}
		row = event.ToAfterMap()
	case "delete": // delete 和 insert，update 在同一事务中，会报错load request not equal last，所以建议单独事务处理
		if len(event.BeforeRow) == 0 {
			return fmt.Errorf("starrocks load failed: action is delete event cannot be empty")
		}
		row = event.ToBeforeMap()
		headers["columns"] = "__op='delete'"
	default:
		e, _ := json.Marshal(event)
		log.Printf("unknown action: %s, event: %s", event.Action, string(e))
		return nil
	}
	body, err := json.Marshal([]map[string]interface{}{row})
	if err != nil {
		return err
	}

	host := output.conf.Starrocks.FeHost
	port := output.conf.Starrocks.FeHttpPort
	loadURL := fmt.Sprintf("http://%s:%d/api/transaction/load", host, port)

	req := nhttp.NewRequest()

	req.CheckRedirect(func(req *http.Request, via []*http.Request) error {

		if "http://182.92.121.152:8030/api/transaction/load" != req.Referer() {
			return nil
		}
		println("重定向URL:", req.URL.String())
		u := req.URL.String()
		u = strings.ReplaceAll(u, "172.17.205.153", "182.92.121.152")

		newUrl, _ := url.Parse(u)
		req.URL = newUrl
		// Go 重定向到不同 host 时会自动剥离 Authorization，
		// 但 CheckRedirect 在剥离之后、发送之前执行，手动补回即可
		req.Header.Set("Authorization", headers["Authorization"])
		req.Header.Set("Expect", "100-continue")
		return nil
	})
	response, err := req.Header(headers).Timeout(60).Do("PUT", loadURL, strings.NewReader(string(body)))
	if err != nil {
		return err
	}
	// log.Printf("[load] label=%s body=%s; response=%s;", label, string(body), string(response.Body))
	result, err := parseResponse(response)
	if err != nil {
		return err
	}
	return output.parseTransactionResult("load", result)
}

// 预提交事务，相当于暂存，集群奔溃后可以恢复（对于事物不是必须的的）
func (output *StarrocksOutput) prepareTransaction(label string) error {
	headers := output.transactionHeaders(label)
	host := output.conf.Starrocks.FeHost
	port := output.conf.Starrocks.FeHttpPort
	loadURL := fmt.Sprintf("http://%s:%d/api/transaction/prepare", host, port)

	req := nhttp.NewRequest()

	req.CheckRedirect(func(req *http.Request, via []*http.Request) error {

		if "http://182.92.121.152:8030/api/transaction/prepare" != req.Referer() {
			return nil
		}
		println("重定向URL:", req.URL.String())
		u := req.URL.String()
		u = strings.ReplaceAll(u, "172.17.205.153", "182.92.121.152")

		newUrl, _ := url.Parse(u)
		req.URL = newUrl
		// Go 重定向到不同 host 时会自动剥离 Authorization，
		// 但 CheckRedirect 在剥离之后、发送之前执行，手动补回即可
		req.Header.Set("Authorization", headers["Authorization"])
		req.Header.Set("Expect", "100-continue")
		return nil
	})

	response, err := req.Header(headers).Timeout(30).Do("POST", loadURL, nil)
	if err != nil {
		return err
	}
	result, err := parseResponse(response)
	if err != nil {
		return err
	}
	return output.parseTransactionResult("prepare", result)
}

// commitTransaction 提交事务并持久化已写入数据。
func (output *StarrocksOutput) commitTransaction(label string) error {
	headers := output.transactionHeaders(label)
	host := output.conf.Starrocks.FeHost
	port := output.conf.Starrocks.FeHttpPort
	loadURL := fmt.Sprintf("http://%s:%d/api/transaction/commit", host, port)

	req := nhttp.NewRequest()

	req.CheckRedirect(func(req *http.Request, via []*http.Request) error {

		if "http://182.92.121.152:8030/api/transaction/commit" != req.Referer() {
			return nil
		}
		println("重定向URL:", req.URL.String())
		u := req.URL.String()
		u = strings.ReplaceAll(u, "172.17.205.153", "182.92.121.152")

		newUrl, _ := url.Parse(u)
		req.URL = newUrl
		// Go 重定向到不同 host 时会自动剥离 Authorization，
		// 但 CheckRedirect 在剥离之后、发送之前执行，手动补回即可
		req.Header.Set("Authorization", headers["Authorization"])
		req.Header.Set("Expect", "100-continue")
		return nil
	})

	response, err := req.Header(headers).Timeout(30).Do("POST", loadURL, nil)
	if err != nil {
		return err
	}
	// log.Printf("[commit] label=%s response=%s;", label, string(response.Body))
	result, err := parseResponse(response)
	if err != nil {
		return err
	}
	return output.parseTransactionResult("commit", result)
}

// rollbackTransaction 回滚事务，用于任一步骤失败后的补偿。
func (output *StarrocksOutput) rollbackTransaction(label string) error {
	headers := output.transactionHeaders(label)
	host := output.conf.Starrocks.FeHost
	port := output.conf.Starrocks.FeHttpPort
	loadURL := fmt.Sprintf("http://%s:%d/api/transaction/rollback", host, port)
	response, err := nhttp.NewRequest().Header(headers).Timeout(30).Do("POST", loadURL, nil)
	if err != nil {
		return err
	}
	result, err := parseResponse(response)
	if err != nil {
		return err
	}
	return output.parseTransactionResult("rollback", result)
}

// parseResponse 解析 StarRocks JSON 响应，保留原始 body 便于排障。
func parseResponse(response nhttp.Response) (map[string]interface{}, error) {
	var result map[string]interface{}
	body := response.Body
	err := json.Unmarshal(body, &result)
	if err != nil {
		return nil, fmt.Errorf("parse response failed: status=%d body=%s err=%w", response.Status, string(body), err)
	}

	return result, err
}

func (output *StarrocksOutput) Buffer(event *input.Event) (map[string]string, error) {
	output.buffers = append(output.buffers, event)
	return output.Flush(false)
}

// Flush 按条件触发写入：数量达标或时间超时任一满足即刷。
// force 强制提交，不考虑批量条件，常用于定时器触发或程序退出前的最终提交。
func (output *StarrocksOutput) Flush(force bool) (map[string]string, error) {
	output.mu.Lock()
	defer output.mu.Unlock()
	if len(output.buffers) == 0 {
		return nil, nil
	}

	// 触发批量条件：数量达标 或 时间超时，任一满足即 flush
	if !force && len(output.buffers) < output.batchSize && time.Since(output.bufferTime) < DefaultMaxWaitDuration {
		return nil, nil
	}

	positionMap := GetPosition(output.buffers)

	groupEvents := make(map[string][]*input.Event) // 按 sourceTable 分组事件
	for _, event := range output.buffers {
		// if event.SourceTable == event.Table {
		// 	groupEvents["main"] = append(groupEvents["main"], event)
		// } else { //适用于表A修改了，要修改表B的情况
		// 	key := strings.Join(event.Columns, ",")
		// 	groupEvents[key] = append(groupEvents[key], event)
		// }

		switch event.Action {
		case "delete":
			groupEvents["delete"] = append(groupEvents["delete"], event)
		case "insert", "update":
			groupEvents["upsert"] = append(groupEvents["upsert"], event)
		}
	}

	for action, events := range groupEvents {
		// 生成全局唯一的事务标签，便于日志追踪和排障。
		label := "nsync_" + strconv.FormatInt(time.Now().UnixNano(), 10)
		count := len(events)
		log.Printf("[txn] label=%s count=%d ;action=%s status=begin", label, count, action)
		if err := output.beginTransaction(label, events[0].Table); err != nil {
			log.Printf("[txn] label=%s count=%d status=FAIL step=begin reason=%v", label, count, err)
			return nil, err
		}

		for _, event := range events {
			e, _ := json.Marshal(event)
			if err := output.loadTransaction(label, event); err != nil {
				log.Printf("[txn] label=%s count=%d; event=%s; status=FAIL step=load reason=%v", label, count, string(e), err)
				rollbackErr := output.rollbackTransaction(label)
				if rollbackErr != nil {
					return nil, fmt.Errorf("%w; rollback error: %v", err, rollbackErr)
				}
				return nil, err
			}
		}
		// if err := output.prepareTransaction(label); err != nil {
		// 	rollbackErr := output.rollbackTransaction(label)
		// 	if rollbackErr != nil {
		// 		return nil, fmt.Errorf("%w; rollback error: %v", err, rollbackErr)
		// 	}
		// 	return nil, err
		// }
		if err := output.commitTransaction(label); err != nil {
			log.Printf("[txn] label=%s count=%d status=FAIL step=commit reason=%v", label, count, err)
			rollbackErr := output.rollbackTransaction(label)
			if rollbackErr != nil {
				return nil, fmt.Errorf("%w; rollback error: %v", err, rollbackErr)
			}
			return nil, err
		}
		log.Printf("[txn] label=%s event=%s count=%d ;action=%s status=end", label, events[0].SourceTable, count, action)
	}

	// 清空缓冲
	output.buffers = output.buffers[:0]
	// Reset batch time
	output.bufferTime = time.Now()
	return positionMap, nil
}

func (output *StarrocksOutput) CountBufferEvent() int {
	output.mu.Lock()
	defer output.mu.Unlock()
	return len(output.buffers)
}
