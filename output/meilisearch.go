package output

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-lazyer/nsync/config"
	"github.com/go-lazyer/nsync/input"
	"github.com/meilisearch/meilisearch-go"
)

const PLACE_HOLDER_GO = "|" //

type MeiliSearchOutput struct {
	client     meilisearch.ServiceManager
	buffers    []*input.Event //mysql
	bufferTime time.Time      // 每个表的批次时间
	conf       *config.OutputConfig
	mu         sync.Mutex //互斥锁
}

func (meiliSearch *MeiliSearchOutput) NewOutput(cfg *config.OutputConfig) error {
	client := meilisearch.New(cfg.MeiliSearch.Origin, meilisearch.WithAPIKey(cfg.MeiliSearch.AdminApiKey))
	meiliSearch.client = client
	version, err := client.Version()
	if err != nil {
		return err
	}
	fmt.Printf("MeiliSearch version: %s\n", version.PkgVersion)
	// 初始化缓冲区
	meiliSearch.buffers = make([]*input.Event, 0)
	// 初始化缓冲区时间
	meiliSearch.bufferTime = time.Now()
	return nil
}

// 只有保存成功才返回position，
func (meiliSearch *MeiliSearchOutput) Buffer(event *input.Event) (position string, err error) {
	meiliSearch.mu.Lock()
	defer meiliSearch.mu.Unlock()

	meiliSearch.buffers = append(meiliSearch.buffers, event)

	// 触发批量条件
	// 先检查数量触发
	if len(meiliSearch.buffers) >= DefaultMaxBatchSize {
		err = meiliSearch.Flush()
		if err == nil {
			return event.Position, nil
		}
	}
	// 时间触发
	if time.Since(meiliSearch.bufferTime) >= DefaultMaxWaitDuration {
		err = meiliSearch.Flush()
		if err == nil {
			return event.Position, nil
		}
	}
	return "", err
}

func (meiliSearch *MeiliSearchOutput) Flush() error {

	if len(meiliSearch.buffers) == 0 {
		return nil
	}
	if len(meiliSearch.buffers) == 0 {
		return nil
	}

	// 按操作类型分组
	for _, event := range meiliSearch.buffers {
		switch event.Action {
		case "update":
			if err := meiliSearch.updateEvent(event); err != nil {
				return err
			}
		case "insert":
			if err := meiliSearch.insertEvent(event); err != nil {
				return err
			}
		case "delete":
			if err := meiliSearch.deleteEvent(event); err != nil {
				return err
			}
		}

	}
	// 清空缓冲
	meiliSearch.buffers = meiliSearch.buffers[:0]
	// Reset batch time
	meiliSearch.bufferTime = time.Time{}
	return nil
}
func (meiliSearch *MeiliSearchOutput) insertEvent(event *input.Event) error {
	if event == nil || len(event.Rows) == 0 {
		return nil
	}
	var builder bytes.Buffer

	// 写入字符串
	builder.WriteString(strings.Join(event.Columns, PLACE_HOLDER_GO) + "\n")
	for _, row := range event.Rows {
		strRow := make([]string, len(row))
		for i, v := range row {
			strRow[i] = Cleaning(v)
		}
		builder.WriteString(strings.Join(strRow, PLACE_HOLDER_GO) + "\n")
	}
	_, err := meiliSearch.client.Index(event.Table).AddDocumentsCsv(builder.Bytes(), &meilisearch.CsvDocumentsQuery{PrimaryKey: event.PrimaryKey[0], CsvDelimiter: PLACE_HOLDER_GO})
	if err != nil {
		return fmt.Errorf("meilisearch insert error:table=%v;documents=%v; %v", event.SourceTable, event.Rows, err)
	}
	return nil
}

// updateEvent 单条更新事件
func (meiliSearch *MeiliSearchOutput) updateEvent(event *input.Event) error {
	if event == nil || len(event.Rows) == 0 {
		return nil
	}
	var builder bytes.Buffer

	// 写入字符串
	builder.WriteString(strings.Join(event.Columns, PLACE_HOLDER_GO) + "\n")
	//只取单数的记录
	for n := 1; n < len(event.Rows); n += 2 {
		strRow := make([]string, len(event.Rows[n]))
		for i, v := range event.Rows[n] {
			strRow[i] = Cleaning(v)
		}
		builder.WriteString(strings.Join(strRow, PLACE_HOLDER_GO) + "\n")
	}
	_, err := meiliSearch.client.Index(event.Table).UpdateDocumentsCsv(builder.Bytes(), &meilisearch.CsvDocumentsQuery{PrimaryKey: event.PrimaryKey[0], CsvDelimiter: PLACE_HOLDER_GO})
	if err != nil {
		return fmt.Errorf("meilisearch update error:table=%v;documents=%v; %v", event.SourceTable, event.Rows, err)
	}
	return nil
}

// deleteEvent 单条删除事件
func (meiliSearch *MeiliSearchOutput) deleteEvent(event *input.Event) error {
	if event == nil || len(event.Rows) == 0 {
		return nil
	}
	primaryKey := event.PrimaryKey[0]

	var primaryKeyOffset int = 0

	// 找到主键在列中的位置
	for n, row := range event.Columns {
		if primaryKey == row {
			primaryKeyOffset = n
			break
		}
	}

	identifiers := make([]string, 0, len(event.Rows))
	// 提取主键值
	for _, row := range event.Rows {
		identifiers = append(identifiers, fmt.Sprintf("%v", row[primaryKeyOffset]))
	}

	_, err := meiliSearch.client.Index(event.Table).DeleteDocuments(identifiers)
	if err != nil {
		return fmt.Errorf("meilisearch delete error:table=%v;documents=%v; %v", event.SourceTable, event.Rows, err)
	}
	return nil
}
func Cleaning(data any) string {
	if data == nil {
		return ""
	}
	st := fmt.Sprintf("%v", data)
	//替换掉换行符
	st = strings.ReplaceAll(st, "\n", "")
	st = strings.ReplaceAll(st, "\r", "")
	st = strings.ReplaceAll(st, "\"", "")
	st = strings.ReplaceAll(st, "|", " ")
	return st
}
