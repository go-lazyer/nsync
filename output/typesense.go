package output

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-lazyer/north/nhttp"
	"github.com/go-lazyer/north/ntime"
	"github.com/go-lazyer/north/ntype"
	"github.com/go-lazyer/nsync/config"
	"github.com/go-lazyer/nsync/constant"
	"github.com/go-lazyer/nsync/input"
	_ "github.com/go-sql-driver/mysql"
)

type TypesenseOutput struct {
	origin     string
	apikey     string
	buffers    []*input.Event //mysql
	bufferTime time.Time      // 每个表的批次时间
	conf       *config.OutputConfig
	mu         sync.Mutex //互斥锁
	batchSize  int        //互斥锁
	columnType map[string]map[string]string
}

func (output *TypesenseOutput) NewOutput(cfg *config.OutputConfig) error {
	output.conf = cfg

	output.origin = fmt.Sprintf("http://%s:%v", cfg.Typesense.Host, cfg.Typesense.Port)
	output.apikey = cfg.Typesense.Apikey
	// 初始化缓冲区
	output.buffers = make([]*input.Event, 0)
	// 初始化缓冲区时间
	output.bufferTime = time.Now()
	output.batchSize = cfg.Typesense.BatchSize

	columnType, err := output.collections()
	if err != nil {
		return err
	}
	output.columnType = columnType
	return nil
}

// 只有保存成功才返回position，
func (output *TypesenseOutput) Buffer(event *input.Event) (position string, err error) {
	output.mu.Lock()
	defer output.mu.Unlock()
	output.setDefault(event)
	//1、非主键批量更新/删除，提前提交
	if event.Columns[event.ColumnPkeys[0]] != "id" {
		err = output.Flush()
		if err != nil {
			return "", err
		}
		err = output.FlushByQuery(event)
		if err != nil {
			return "", err
		}
		return event.Position, nil
	}
	//2、action为delete，提前提交
	if event.Action == "delete" {
		err = output.Flush()
		if err != nil {
			return "", err
		}
		err = output.deleteEvent(event)
		if err != nil {
			return "", err
		}
		return event.Position, nil
	}
	//3、更换表，提前提交
	if len(output.buffers) > 0 && output.buffers[0].Table != event.Table {
		err = output.Flush()
		if err != nil {
			return "", err
		}
		output.buffers = append(output.buffers, event)
		return event.Position, nil
	}

	output.buffers = append(output.buffers, event)
	batchSize := DefaultMaxBatchSize
	if output.batchSize > 0 {
		batchSize = output.batchSize
	}

	// 触发批量条件
	// 先检查数量触发
	if len(output.buffers) >= batchSize {
		err = output.Flush()
		if err == nil {
			return event.Position, nil
		}
	}
	// 时间触发
	if time.Since(output.bufferTime) >= DefaultMaxWaitDuration {
		err = output.Flush()
		if err == nil {
			return event.Position, nil
		}
	}
	return "", err
}

func (output *TypesenseOutput) FlushByQuery(event *input.Event) error {
	switch event.Action {
	case "update": // 不能更新全文字段或列存属性
		if err := output.updateByQuery(event); err != nil {
			return err
		}
	case "delete":
		if err := output.deleteByQuery(event); err != nil {
			return err
		}
	}
	return nil
}
func (output *TypesenseOutput) Flush() error {

	if len(output.buffers) == 0 {
		return nil
	}

	err := output.Import(output.buffers)
	if err != nil {
		return err
	}

	// 清空缓冲
	output.buffers = output.buffers[:0]
	// Reset batch time
	output.bufferTime = time.Now()
	return nil
}

func (output *TypesenseOutput) Close() error {
	return nil
}

// deleteEvent 单条删除事件
func (output *TypesenseOutput) deleteEvent(event *input.Event) error {
	if event == nil || len(event.BeforeRow) == 0 {
		return nil
	}
	header := map[string]string{
		"X-TYPESENSE-API-KEY": output.apikey,
	}
	u := fmt.Sprintf("%s/collections/%s/documents%s", output.origin, event.Table, event.BeforeRow[event.ColumnPkeys[0]])
	response, err := nhttp.NewRequest().Header(header).Delete(u)
	if err != nil {
		return fmt.Errorf("client.Do error %v", err)
	}
	resMap := make(map[string]any)
	err = json.Unmarshal(response.Body, &resMap)
	if err != nil {
		return fmt.Errorf("deleteEvent json.Unmarshal error %v", err)
	}
	success := resMap["success"]
	if success == "false" {
		return fmt.Errorf("request=%v; response error %v", u, string(response.Body))
	}
	return nil
}
func (output *TypesenseOutput) Import(events []*input.Event) error {
	if events == nil {
		return nil
	}
	ndjson := make([]string, 0)
	for _, event := range events {
		switch event.Action {
		case "update":
			afterMap := event.ToAfterMap()
			d, _ := json.Marshal(afterMap)
			ndjson = append(ndjson, string(d))
		case "insert":
			afterMap := event.ToAfterMap()
			d, _ := json.Marshal(afterMap)
			ndjson = append(ndjson, string(d))
		}
	}

	header := map[string]string{
		"X-TYPESENSE-API-KEY": output.apikey,
		"Content-Type":        "text/plain; charset=utf-8",
	}

	u := fmt.Sprintf("%s/collections/%s/documents/import?action=emplace", output.origin, events[0].Table)
	response, err := nhttp.NewRequest().Header(header).PostText(u, strings.Join(ndjson, "\n"))
	if err != nil {
		return fmt.Errorf("client.Do error %v", err)
	}
	responses := strings.Split(string(response.Body), "\n")

	errs := make([]string, 0)
	for n, res := range responses {
		if res == "{\"success\":true}" {
			continue
		}
		errs = append(errs, fmt.Sprintf("request=%v; response%v", ndjson[n], res))
	}
	if len(errs) > 0 {
		return fmt.Errorf("Import  error %v", strings.Join(errs, "\n"))
	}
	return nil
}
func (output *TypesenseOutput) updateByQuery(event *input.Event) error {
	if event == nil {
		return nil
	}
	afterMap := event.ToAfterMap()
	delete(afterMap, event.Columns[event.ColumnPkeys[0]])
	d, _ := json.Marshal(afterMap)

	header := map[string]string{
		"X-TYPESENSE-API-KEY": output.apikey,
	}
	u := fmt.Sprintf("%s/collections/%s/documents?filter_by=%s:=%s", output.origin, event.Table, event.Columns[event.ColumnPkeys[0]], event.AfterRow[event.ColumnPkeys[0]])
	response, err := nhttp.NewRequest().Header(header).PatchJson(u, string(d))
	if err != nil {
		return fmt.Errorf("client.Do error %v", err)
	}
	resMap := make(map[string]any)
	err = json.Unmarshal(response.Body, &resMap)
	if err != nil {
		return fmt.Errorf("updateByQuery json.Unmarshal error %v", err)
	}
	success := resMap["success"]
	if success == "false" {
		return fmt.Errorf("request=%v; response error %v", u, string(response.Body))
	}
	return nil
}
func (output *TypesenseOutput) deleteByQuery(event *input.Event) error {
	if event == nil {
		return nil
	}
	header := map[string]string{
		"X-TYPESENSE-API-KEY": output.apikey,
	}
	u := fmt.Sprintf("%s/collections/%s/documents?filter_by=%s:=%s", output.origin, event.Table, event.Columns[event.ColumnPkeys[0]], event.AfterRow[event.ColumnPkeys[0]])
	response, err := nhttp.NewRequest().Header(header).Delete(u)
	if err != nil {
		return fmt.Errorf("client.Do error %v", err)
	}
	resMap := make(map[string]any)
	err = json.Unmarshal(response.Body, &resMap)
	if err != nil {
		return fmt.Errorf("deleteByQuery json.Unmarshal error %v", err)
	}
	success := resMap["success"]
	if success == "false" {
		return fmt.Errorf("request=%v; response error %v", u, string(response.Body))
	}
	return nil
}

func (output *TypesenseOutput) collections() (map[string]map[string]string, error) {

	header := map[string]string{
		"X-TYPESENSE-API-KEY": output.apikey,
	}
	u := fmt.Sprintf("%s/collections", output.origin)
	response, err := nhttp.NewRequest().Header(header).Get(u)
	if err != nil {
		return nil, fmt.Errorf("client.Do error %v", err)
	}
	// Parse response into a slice of collections; Typesense returns an array here.
	res := make([]map[string]any, 0)
	err = json.Unmarshal(response.Body, &res)
	if err != nil {
		return nil, fmt.Errorf("collections json.Unmarshal error %v", err)
	}

	columnTypes := make(map[string]map[string]string)
	for _, collection := range res {
		fields := collection["fields"].([]any)

		columnType := make(map[string]string)
		for _, field := range fields {
			if fieldMap, ok := field.(map[string]any); ok {
				columnType[ntype.ToString(fieldMap["name"])] = ntype.ToString(fieldMap["type"])
			}
		}
		columnTypes[ntype.ToString(collection["name"])] = columnType
	}
	return columnTypes, nil
}

func (output *TypesenseOutput) setDefault(event *input.Event) {
	if event.Action == "insert" {
		columnKeys := event.GetKeysByColumns()

		for column, columnType := range output.columnType[event.Table] {
			if _, ok := columnKeys[column]; ok {
				continue
			}
			event.Columns = append(event.Columns, column)
			event.ColumnTypes = append(event.ColumnTypes, columnType)

			nsyncType := constant.TypesenseColumnType[columnType]
			if nsyncType == "" {
				event.AfterRow = append(event.AfterRow, "")
			} else {
				event.AfterRow = append(event.AfterRow, constant.NsyncColumnTypes[nsyncType].Default)
			}
		}
	}

	for i := 0; i < len(event.AfterRow); i++ {
		if constant.IsDateType(event.ColumnTypes[i]) {
			valus := event.AfterRow[i]
			if valus == nil {
				valus = "1970-01-01T00:00:00"
			}
			event.AfterRow[i] = ntime.Parse(ntype.ToString(valus)).Unix()
		} else if constant.IsStringType(event.ColumnTypes[i]) {
			valus := ""
			if event.AfterRow[i] == nil {
				valus = ""
			}
			if ntype.IsSlice(valus) {
				valus = string(event.AfterRow[i].([]byte))
			} else {
				valus = ntype.ToString(event.AfterRow[i])
			}

			if json.Valid([]byte(valus)) {
				event.AfterRow[i] = valus
			} else {
				event.AfterRow[i] = CleanSpecialChars(valus)
			}
		} else if constant.IsMultiType(event.ColumnTypes[i]) {
			// event.AfterRow[i] = fmt.Sprintf("(%v)", event.AfterRow[i])
		} else {
			if event.AfterRow[i] == nil {
				event.AfterRow[i] = 0
			}
		}
	}
	for i := 0; i < len(event.BeforeRow); i++ {
		if constant.IsDateType(event.ColumnTypes[i]) {
			valus := event.BeforeRow[i]
			if valus == nil {
				valus = "1970-01-01T00:00:00"
			}
			event.BeforeRow[i] = ntime.Parse(ntype.ToString(valus)).Unix()
		} else if constant.IsStringType(event.ColumnTypes[i]) {
			valus := ""
			if event.BeforeRow[i] == nil {
				valus = ""
			}
			if ntype.IsSlice(valus) {
				valus = string(event.BeforeRow[i].([]byte))
			} else {
				valus = ntype.ToString(event.BeforeRow[i])
			}

			if json.Valid([]byte(valus)) {
				event.BeforeRow[i] = valus
			} else {
				event.BeforeRow[i] = CleanSpecialChars(valus)
			}
		} else if constant.IsMultiType(event.ColumnTypes[i]) {
			// event.BeforeRow[i] = fmt.Sprintf("(%v)", event.BeforeRow[i])
		} else {
			if event.BeforeRow[i] == nil {
				event.BeforeRow[i] = 0
			}
		}
	}
}
