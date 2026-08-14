package output

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
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
	headers["table"] = table
	host := output.conf.Starrocks.Host
	port := output.conf.Starrocks.Port
	loadURL := fmt.Sprintf("http://%s:%d/api/transaction/begin", host, port)
	response, err := nhttp.NewRequest().Header(headers).Timeout(30).Do("POST", loadURL, nil)
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
		return nil
	}

	if event.Table == "" {
		return fmt.Errorf("starrocks load failed: empty table")
	}

	var row map[string]interface{}
	headers := output.transactionHeaders(label)
	headers["table"] = event.Table
	headers["format"] = "json"
	headers["strip_outer_array"] = "true"

	switch event.Action {
	case "insert":
		if len(event.AfterRow) == 0 {
			return nil
		}
		row = event.ToAfterMap()
	case "update":
		if len(event.AfterRow) == 0 {
			return nil
		}
		row = event.ToAfterMap()
		// update 语义按 partial update 发送。
		headers["partial_update"] = "true"
	case "delete":
		if len(event.BeforeRow) == 0 {
			return nil
		}
		row = event.ToBeforeMap()
	default:
		return nil
	}

	body, err := json.Marshal([]map[string]interface{}{row})
	if err != nil {
		return err
	}

	host := output.conf.Starrocks.Host
	port := output.conf.Starrocks.Port
	loadURL := fmt.Sprintf("http://%s:%d/api/transaction/load", host, port)
	response, err := nhttp.NewRequest().Header(headers).Timeout(30).Do("PUT", loadURL, strings.NewReader(string(body)))
	if err != nil {
		return err
	}
	result, err := parseResponse(response)
	if err != nil {
		return err
	}
	return output.parseTransactionResult("load", result)
}

// prepareTransaction 将事务从 PREPARE 推进到 PREPARED。
func (output *StarrocksOutput) prepareTransaction(label string) error {
	headers := output.transactionHeaders(label)
	host := output.conf.Starrocks.Host
	port := output.conf.Starrocks.Port
	loadURL := fmt.Sprintf("http://%s:%d/api/transaction/prepare", host, port)
	response, err := nhttp.NewRequest().Header(headers).Timeout(30).Do("POST", loadURL, nil)
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
	host := output.conf.Starrocks.Host
	port := output.conf.Starrocks.Port
	loadURL := fmt.Sprintf("http://%s:%d/api/transaction/commit", host, port)
	response, err := nhttp.NewRequest().Header(headers).Timeout(30).Do("POST", loadURL, nil)
	if err != nil {
		return err
	}
	result, err := parseResponse(response)
	if err != nil {
		return err
	}
	return output.parseTransactionResult("commit", result)
}

// rollbackTransaction 回滚事务，用于任一步骤失败后的补偿。
func (output *StarrocksOutput) rollbackTransaction(label string) error {
	headers := output.transactionHeaders(label)
	host := output.conf.Starrocks.Host
	port := output.conf.Starrocks.Port
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

func (output *StarrocksOutput) Buffer(event *input.Event) (position string, err error) {
	output.buffers = append(output.buffers, event)
	return output.Flush(false)
}

// Flush 按条件触发写入：数量达标或时间超时任一满足即刷。
func (output *StarrocksOutput) Flush(force bool) (string, error) {
	output.mu.Lock()
	defer output.mu.Unlock()
	if len(output.buffers) == 0 {
		return "", nil
	}

	// 触发批量条件
	// 先检查数量触发
	if !force && len(output.buffers) < output.batchSize {
		return "", nil
	}
	// 时间触发
	if !force && time.Since(output.bufferTime) < DefaultMaxWaitDuration {
		return "", nil
	}

	// 生成全局唯一的事务标签，便于日志追踪和排障。
	label := "nsync_" + strconv.FormatInt(time.Now().UnixNano(), 10)

	if err := output.beginTransaction(label, output.buffers[0].Table); err != nil {
		return "", err
	}
	position := output.buffers[len(output.buffers)-1].Position
	for _, event := range output.buffers {
		if err := output.loadTransaction(label, event); err != nil {
			rollbackErr := output.rollbackTransaction(label)
			if rollbackErr != nil {
				return "", fmt.Errorf("%w; rollback error: %v", err, rollbackErr)
			}
			return "", err
		}
	}

	if err := output.prepareTransaction(label); err != nil {
		rollbackErr := output.rollbackTransaction(label)
		if rollbackErr != nil {
			return "", fmt.Errorf("%w; rollback error: %v", err, rollbackErr)
		}
		return "", err
	}

	if err := output.commitTransaction(label); err != nil {
		rollbackErr := output.rollbackTransaction(label)
		if rollbackErr != nil {
			return "", fmt.Errorf("%w; rollback error: %v", err, rollbackErr)
		}
		return "", err
	}

	// 清空缓冲
	output.buffers = output.buffers[:0]
	// Reset batch time
	output.bufferTime = time.Now()
	return position, nil
}

func (output *StarrocksOutput) CountBufferEvent() int {
	output.mu.Lock()
	defer output.mu.Unlock()
	return len(output.buffers)
}
