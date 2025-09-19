package output

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-lazyer/nsync/config"
	"github.com/go-lazyer/nsync/input"
)

type StarrocksOutput struct {
	db         *sql.DB
	auth       string
	buffers    []*input.Event //mysql
	bufferTime time.Time      // 每个表的批次时间
	conf       *config.OutputConfig
	mu         sync.Mutex //互斥锁
}

func (starrocks *StarrocksOutput) NewOutput(cfg *config.OutputConfig) error {
	starrocks.conf = cfg
	s := cfg.Starrocks.Username + ":" + cfg.Starrocks.Password
	b := []byte(s)
	starrocks.auth = base64.StdEncoding.EncodeToString(b)

	return nil
}

func (starrocks *StarrocksOutput) StreamLoad(json string, table string) error {
	host := starrocks.conf.Starrocks.Host
	port := starrocks.conf.Starrocks.Port
	database := starrocks.conf.Starrocks.Database

	client := &http.Client{}
	loadUrl := fmt.Sprintf("http://%s:%d/api/%s/%s/_stream_load", host, port, database, table)
	req, err := http.NewRequest("PUT", loadUrl, strings.NewReader(json))
	if err != nil {
		fmt.Println(err)
	}
	req.Header.Add("Authorization", "Basic "+starrocks.auth)
	req.Header.Add("Expect", "100-continue")
	req.Header.Add("format", "json")
	req.Header.Add("strip_outer_array", "true")

	response, err := client.Do(req)
	if err != nil {
		return err
	}
	returnMap, err := parseResponse(response)
	if returnMap["Status"] != "Success" {
		msg := returnMap["Message"]
		return errors.New(msg.(string))
	}
	return nil
}

func parseResponse(response *http.Response) (map[string]interface{}, error) {
	var result map[string]interface{}
	body, err := io.ReadAll(response.Body)
	fmt.Println(string(body))
	if err == nil {
		err = json.Unmarshal(body, &result)
	}

	return result, err
}

// 只有保存成功才返回position，
func (starrocks *StarrocksOutput) Buffer(event *input.Event) (position string, err error) {
	starrocks.mu.Lock()
	defer starrocks.mu.Unlock()

	starrocks.buffers = append(starrocks.buffers, event)

	// 触发批量条件
	// 先检查数量触发
	if len(starrocks.buffers) >= DefaultMaxBatchSize {
		err = starrocks.Flush()
		if err == nil {
			return event.Position, nil
		}
	}
	// 时间触发
	if time.Since(starrocks.bufferTime) >= DefaultMaxWaitDuration {
		err = starrocks.Flush()
		if err == nil {
			return event.Position, nil
		}
	}
	return "", err
}

func (starrocks *StarrocksOutput) Flush() error {
	// mysql.mu.Lock()
	// defer mysql.mu.Unlock()

	if len(starrocks.buffers) == 0 {
		return nil
	}
	tx, err := starrocks.db.Begin()
	if err != nil {
		return err
	}

	// 按操作类型分组
	for _, event := range starrocks.buffers {
		switch event.Action {
		case "update":
			// if err := starrocks.updateEvent(tx, event); err != nil {
			// 	tx.Rollback()
			// 	return err
			// }
		case "insert":
			// if err := starrocks.insertEvent(tx, event); err != nil {
			// 	tx.Rollback()
			// 	return err
			// }
		case "delete":
			// if err := starrocks.deleteEvent(tx, event); err != nil {
			// 	tx.Rollback()
			// 	return err
			// }
		}

	}
	if err := tx.Commit(); err != nil {
		return err
	}
	// 清空缓冲
	starrocks.buffers = starrocks.buffers[:0]
	// Reset batch time
	starrocks.bufferTime = time.Time{}
	return nil
}
