package output

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-lazyer/north/ntime"
	"github.com/go-lazyer/north/ntype"
	"github.com/go-lazyer/nsync/config"
	"github.com/go-lazyer/nsync/constant"
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
			if err := meiliSearch.updateJsonEvent(event); err != nil {
				return err
			}
		case "insert":
			if err := meiliSearch.insertJsonEvent(event); err != nil {
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
func (meiliSearch *MeiliSearchOutput) insertCsvEvent(event *input.Event) error {
	if event == nil || len(event.AfterRow) == 0 {
		return nil
	}
	var builder bytes.Buffer

	// 写入列名"id:number","title:string","genres:string","release-year:number"
	columns := make([]string, len(event.Columns))
	for i, column := range event.Columns {
		columns[i] = fmt.Sprintf("%s:%s", column, constant.NsyncColumnTypes[event.ColumnTypes[i]].Meilisearch)
	}
	builder.WriteString(strings.Join(columns, PLACE_HOLDER_GO) + "\n")

	dateColumes := make(map[int]bool, 0)
	for i, t := range event.ColumnTypes {
		if constant.IsDateType(t) {
			dateColumes[i] = true
		}
	}

	//写入数据；1.过滤特殊字符 2.时间类型转换为时间戳
	strRow := make([]string, len(event.AfterRow))
	for i, v := range event.AfterRow {
		st := Cleaning(fmt.Sprintf("%v", v))
		if dateColumes[i] && st != "" {
			strRow[i] = ntype.ToString(ntime.Parse(st).Unix())
		} else {
			strRow[i] = st
		}
	}
	builder.WriteString(strings.Join(strRow, PLACE_HOLDER_GO))
	_, err := meiliSearch.client.Index(event.Table).AddDocumentsCsv(builder.Bytes(), &meilisearch.CsvDocumentsQuery{PrimaryKey: event.Columns[event.ColumnPkeys[0]], CsvDelimiter: PLACE_HOLDER_GO})
	if err != nil {
		return fmt.Errorf("meilisearch insert error:table=%v;documents=%v; %v", event.SourceTable, event.AfterRow, err)
	}
	return nil
}

// updateCsvEvent 单条更新事件
func (meiliSearch *MeiliSearchOutput) updateCsvEvent(event *input.Event) error {
	if event == nil || len(event.AfterRow) == 0 {
		return nil
	}
	var builder bytes.Buffer

	// 写入字符串
	columns := make([]string, len(event.Columns))
	for i, column := range event.Columns {
		columns[i] = fmt.Sprintf("%s:%s", column, constant.NsyncColumnTypes[event.ColumnTypes[i]].Meilisearch)
	}
	builder.WriteString(strings.Join(columns, PLACE_HOLDER_GO) + "\n")

	dateColumes := make(map[int]bool, 0)
	for i, t := range event.ColumnTypes {
		if constant.IsDateType(t) {
			dateColumes[i] = true
		}
	}
	//只取单数的记录

	strRow := make([]string, len(event.AfterRow))
	for i, v := range event.AfterRow {
		st := Cleaning(fmt.Sprintf("%v", v))
		if dateColumes[i] && st != "" {
			strRow[i] = ntype.ToString(ntime.Parse(st).Unix())
		} else {
			strRow[i] = st
		}
	}
	builder.WriteString(strings.Join(strRow, PLACE_HOLDER_GO) + "\n")
	t, err := meiliSearch.client.Index(event.Table).UpdateDocumentsCsv(builder.Bytes(), &meilisearch.CsvDocumentsQuery{PrimaryKey: event.Columns[event.ColumnPkeys[0]], CsvDelimiter: PLACE_HOLDER_GO})
	if err != nil {
		return fmt.Errorf("meilisearch update error:table=%v;documents=%v; %v", event.SourceTable, event.AfterRow, err)
	}
	fmt.Printf("MeiliSearch update task: %v\n", t)
	return nil
}

func (meiliSearch *MeiliSearchOutput) insertJsonEvent(event *input.Event) error {
	if event == nil || len(event.AfterRow) == 0 {
		return nil
	}

	dateColumes := make(map[int]bool, 0)
	for i, t := range event.ColumnTypes {
		if constant.IsDateType(t) {
			dateColumes[i] = true
		}
	}
	documents := make([]map[string]any, 0)

	//写入数据；1.过滤特殊字符 2.时间类型转换为时间戳
	doc := make(map[string]any)
	for i, v := range event.AfterRow {
		colunType := event.ColumnTypes[i]

		if constant.IsDateType(colunType) && v != nil {
			doc[event.Columns[i]] = ntime.Parse(ntype.ToString(v)).Unix()
		} else if constant.IsStringType(colunType) {
			st := ""
			if ntype.IsSlice(v) {
				st = string(v.([]uint8))
			} else {
				st = fmt.Sprintf("%v", v)
			}
			st = Cleaning(st)
			if event.Columns[i] == "_geo" {
				var lat, lng float64 = 0, 0
				st := strings.Split(st, ",")
				if len(st) == 2 && ntype.IsNumeric(st[0]) && ntype.IsNumeric(st[1]) {
					lat = ntype.ToFloat64(st[0])
					lng = ntype.ToFloat64(st[1])
				}
				doc[event.Columns[i]] = map[string]float64{"lat": lat, "lng": lng} //meilisearch要求_geo字段必须是对象
			} else {
				doc[event.Columns[i]] = st
			}
		} else {
			doc[event.Columns[i]] = v
		}
	}
	documents = append(documents, doc)
	_, err := meiliSearch.client.Index(event.Table).AddDocuments(documents, &event.Columns[event.ColumnPkeys[0]])
	if err != nil {
		return fmt.Errorf("meilisearch insert Json event error:table=%v;documents=%v; %v", event.SourceTable, event.AfterRow, err)
	}
	return nil
}

// updateCsvEvent 单条更新事件
func (meiliSearch *MeiliSearchOutput) updateJsonEvent(event *input.Event) error {
	if event == nil || len(event.AfterRow) == 0 {
		return nil
	}

	documents := make([]map[string]any, 0)

	//写入数据；1.过滤特殊字符 2.时间类型转换为时间戳
	doc := make(map[string]any)
	for i, v := range event.AfterRow {
		colunType := event.ColumnTypes[i]

		if constant.IsDateType(colunType) && v != nil {
			doc[event.Columns[i]] = ntime.Parse(ntype.ToString(v)).Unix()
		} else if constant.IsStringType(colunType) {
			// if event.Columns[i] == "dw_owner" {
			// 	fmt.Println(v)
			// }
			st := ""
			if ntype.IsSlice(v) {
				st = string(v.([]uint8))
			} else {
				st = fmt.Sprintf("%v", v)
			}
			st = Cleaning(st)
			if event.Columns[i] == "_geo" {
				var lat, lng float64 = 0, 0
				st := strings.Split(st, ",")
				if len(st) == 2 && ntype.IsNumeric(st[0]) && ntype.IsNumeric(st[1]) {
					lat = ntype.ToFloat64(st[0])
					lng = ntype.ToFloat64(st[1])
				}
				doc[event.Columns[i]] = map[string]float64{"lat": lat, "lng": lng} //meilisearch要求_geo字段必须是对象
			} else {
				doc[event.Columns[i]] = st
			}
		} else {
			doc[event.Columns[i]] = v
		}
	}
	documents = append(documents, doc)
	_, err := meiliSearch.client.Index(event.Table).UpdateDocuments(documents, &event.Columns[event.ColumnPkeys[0]])
	if err != nil {
		return fmt.Errorf("meilisearch update error:table=%v;documents=%v; %v", event.SourceTable, event.AfterRow, err)
	}
	return nil
}

// deleteEvent 单条删除事件
func (meiliSearch *MeiliSearchOutput) deleteEvent(event *input.Event) error {
	if event == nil || len(event.BeforeRow) == 0 {
		return nil
	}

	identifiers := make([]string, 0)
	// 提取主键值
	identifiers = append(identifiers, fmt.Sprintf("%v", event.BeforeRow[event.ColumnPkeys[0]]))

	_, err := meiliSearch.client.Index(event.Table).DeleteDocuments(identifiers)
	if err != nil {
		return fmt.Errorf("meilisearch delete error:table=%v;documents=%v; %v", event.SourceTable, event.BeforeRow, err)
	}
	return nil
}
func Cleaning(data string) string {
	st := fmt.Sprintf("%v", data)
	//替换掉换行符
	st = strings.ReplaceAll(st, "<null>", "")
	st = strings.ReplaceAll(st, "<nil>", "")
	st = strings.ReplaceAll(st, "null", "")
	st = strings.ReplaceAll(st, "nil", "")
	st = strings.ReplaceAll(st, "\n", "")
	st = strings.ReplaceAll(st, "\r", "")
	st = strings.ReplaceAll(st, "\"", "")
	st = strings.ReplaceAll(st, "|", " ")
	return st
}

// 写一个方法，从channel读取数据 taskid，然后查询任务，如果报错了，打印日志。
func (meiliSearch *MeiliSearchOutput) Close() error {
	meiliSearch.mu.Lock()
	defer meiliSearch.mu.Unlock()
	if len(meiliSearch.buffers) > 0 {
		return meiliSearch.Flush()
	}
	return nil
}
