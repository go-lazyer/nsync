package output

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/go-lazyer/north/nhttp"
	"github.com/go-lazyer/north/nslice"
	"github.com/go-lazyer/north/nsql"
	"github.com/go-lazyer/north/ntime"
	"github.com/go-lazyer/north/ntype"
	"github.com/go-lazyer/nsync/config"
	"github.com/go-lazyer/nsync/constant"
	"github.com/go-lazyer/nsync/input"
	_ "github.com/go-sql-driver/mysql"
)

type ManticoreOutput struct {
	db              *sql.DB
	origin          string
	buffers         []*input.Event //mysql
	bufferTime      time.Time      // 每个表的批次时间
	conf            *config.OutputConfig
	tableColumnType map[string]map[string]string
	mu              sync.Mutex //互斥锁
	batchSize       int        //互斥锁
}

func (output *ManticoreOutput) NewOutput(cfg *config.OutputConfig) error {
	output.conf = cfg
	dsn := fmt.Sprintf("%s:%s@(%s:%v)/%s?charset=utf8mb4&parseTime=True&loc=Local", cfg.Manticore.Username, cfg.Manticore.Password, cfg.Manticore.Host, cfg.Manticore.Port, cfg.Manticore.Database)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	err = db.Ping()
	if err != nil {
		return err
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(2)
	output.db = db

	output.origin = fmt.Sprintf("http://%s:%v", cfg.Manticore.Host, cfg.Manticore.HttpPort)

	// 初始化缓冲区
	output.buffers = make([]*input.Event, 0)
	// 初始化缓冲区时间
	output.bufferTime = time.Now()
	output.batchSize = cfg.Manticore.BatchSize
	if output.batchSize <= 0 {
		output.batchSize = DefaultMaxBatchSize
	}
	columnTypes, err := initTableColumnType(db)
	if err != nil {
		return err
	}
	output.tableColumnType = columnTypes
	return nil
}

func initTableColumnType(db *sql.DB) (map[string]map[string]string, error) {
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		return nil, err
	}
	tableMaps, err := nsql.ScanToMap(rows)
	if err != nil {
		return nil, err
	}
	columnTypes := make(map[string]map[string]string)
	for _, table := range tableMaps {
		rows, err := db.Query(fmt.Sprintf("DESC %s", table["Table"]))
		if err != nil {
			return nil, err
		}
		columns, err := nsql.ScanToMap(rows)
		if err != nil {
			return nil, err
		}
		columnType := make(map[string]string)
		for _, column := range columns {
			columnType[ntype.ToString(column["Field"])] = ntype.ToString(column["Type"])
		}
		columnTypes[ntype.ToString(table["Table"])] = columnType
	}
	return columnTypes, nil
}

// CountBufferEvent returns the number of events currently in the output buffer.
func (output *ManticoreOutput) CountBufferEvent() int {
	return len(output.buffers)
}

// 把事件添加到缓冲区，并根据批量条件触发 Flush
func (output *ManticoreOutput) Buffer(event *input.Event) (position string, err error) {
	output.buffers = append(output.buffers, event)
	return output.Flush(false)
}
func (output *ManticoreOutput) Flush(force bool) (string, error) {
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

	// position 为最后一个事件的 position
	position := output.buffers[len(output.buffers)-1].Position
	err := output.bulk(output.buffers)
	if err != nil {
		return "", err
	}

	// 清空缓冲
	output.buffers = output.buffers[:0]
	// Reset batch time
	output.bufferTime = time.Now()
	return position, nil
}

func (output *ManticoreOutput) FlushAll() (string, error) {
	output.mu.Lock()
	defer output.mu.Unlock()

	if len(output.buffers) == 0 {
		return "", nil
	}
	// position 为最后一个事件的 position
	position := output.buffers[len(output.buffers)-1].Position
	err := output.bulk(output.buffers)
	if err != nil {
		return "", err
	}

	// 清空缓冲
	output.buffers = output.buffers[:0]
	// Reset batch time
	output.bufferTime = time.Now()
	return position, nil
}
func (output *ManticoreOutput) FlushBySql(event *input.Event) error {

	tx, err := output.db.Begin()
	if err != nil {
		return err
	}
	// UPDATE 更新全文字段或列存属性的内容请使用 REPLACE。
	// UPDATE 主键，需要拆分为delete加insert
	// removeUnknownColumn(event, output)
	// setDefault(event)
	// resetAction(event, output)
	switch event.Action {
	case "update": // 不能更新全文字段或列存属性
		if err := output.upsertEvent(tx, event); err != nil {
			tx.Rollback()
			return err
		}
	case "replace": // 不能更新全文字段或列存属性，先查询旧的
		if err := output.replaceEvent(tx, event); err != nil {
			tx.Rollback()
			return err
		}
	case "insert":
		if err := output.insertEvent(tx, event); err != nil {
			tx.Rollback()
			return err
		}
	case "delete":
		if err := output.deleteEvent(tx, event); err != nil {
			tx.Rollback()
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func (output *ManticoreOutput) Close() error {
	return output.db.Close()
}

func (output *ManticoreOutput) upsertEvent(tx *sql.Tx, event *input.Event) error {
	if event == nil || len(event.BeforeRow) == 0 || len(event.AfterRow) == 0 {
		return nil
	}

	sets := make([]string, 0)
	for j, v := range event.AfterRow {

		// 主键不更新
		if event.ColumnPkeys[0] == j {
			continue
		}
		if output.tableColumnType[event.Table][event.Columns[j]] == "text" {
			continue
		}

		if constant.IsStringType(event.ColumnTypes[j]) {
			sets = append(sets, fmt.Sprintf("%s='%v'", event.Columns[j], v))
		} else {
			sets = append(sets, fmt.Sprintf("%s=%v", event.Columns[j], v))
		}

	}
	var where string
	if constant.IsStringType(event.ColumnTypes[event.ColumnPkeys[0]]) {
		where = fmt.Sprintf("%s='%v'", event.Columns[event.ColumnPkeys[0]], event.AfterRow[event.ColumnPkeys[0]])
	} else {
		where = fmt.Sprintf("%s=%v", event.Columns[event.ColumnPkeys[0]], event.AfterRow[event.ColumnPkeys[0]])
	}
	table := event.Table
	if event.Database != "" {
		table = event.Database + ":" + event.Table
	}

	query := fmt.Sprintf("update %s set %s  where %s", table, strings.Join(sets, ","), where)

	_, err := tx.Exec(query)
	if err != nil {
		return err
	}
	return nil
}
func (output *ManticoreOutput) selectEvent(tx *sql.Tx, event *input.Event) (map[string]any, []string, error) {
	if event == nil || len(event.AfterRow) == 0 {
		return nil, nil, nil
	}
	query := fmt.Sprintf("select * from %s where id = %v", event.Table, event.AfterRow[event.ColumnPkeys[0]])
	rows, err := tx.Query(query)
	if err != nil {
		return nil, nil, err
	}
	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}

	rowMaps, err := nsql.ScanToMap(rows)
	if err != nil {
		return nil, nil, err
	}
	if len(rowMaps) == 0 {
		return nil, nil, nil
	}

	return rowMaps[0], columns, nil
}
func (output *ManticoreOutput) replaceEvent(tx *sql.Tx, event *input.Event) error {
	if event == nil || len(event.AfterRow) == 0 {
		return nil
	}

	selectEvent, selectColumns, err := output.selectEvent(tx, event)
	if err != nil {
		return err
	}
	for _, column := range selectColumns {
		if slices.Contains(event.Columns, column) {
			continue
		}
		event.Columns = append(event.Columns, column)
		event.AfterRow = append(event.AfterRow, selectEvent[column])
	}

	columnsClause := strings.Join(event.Columns, ", ")

	// 用于构建 VALUES 部分
	valuePlaceholders := make([]string, 0) // 每一行的 (?, ?, ...)

	placeholders := make([]string, len(event.AfterRow))
	for j, v := range event.AfterRow {
		placeholders[j] = fmt.Sprintf("'%v'", v)
	}
	valuePlaceholders = append(valuePlaceholders, "("+strings.Join(placeholders, ",")+")")

	// 如果没有要插入的数据
	if len(valuePlaceholders) == 0 {
		return nil
	}

	// 构建最终 SQL
	query := fmt.Sprintf("REPLACE INTO %s (%s) VALUES %v", event.Table, columnsClause, strings.Join(valuePlaceholders, ","))
	_, err = tx.Exec(query)
	if err != nil {
		return fmt.Errorf("replace exec err: %v, sql: %s", err, query)
	}
	return nil
}
func (output *ManticoreOutput) insertEvent(tx *sql.Tx, event *input.Event) error {
	if event == nil || len(event.AfterRow) == 0 {
		return nil
	}
	columnsClause := strings.Join(event.Columns, ", ")

	// 用于构建 VALUES 部分
	valuePlaceholders := make([]string, 0) // 每一行的 (?, ?, ...)

	placeholders := make([]string, len(event.AfterRow))
	for i := 0; i < len(event.AfterRow); i++ {
		placeholders[i] = fmt.Sprintf("'%v'", event.AfterRow[i])
	}
	valuePlaceholders = append(valuePlaceholders, "("+strings.Join(placeholders, ",")+")")

	// 如果没有要插入的数据
	if len(valuePlaceholders) == 0 {
		return nil
	}

	// 构建最终 SQL
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %v", event.Table, columnsClause, strings.Join(valuePlaceholders, ","))
	_, err := tx.Exec(query)
	if err != nil {
		return fmt.Errorf("insert exec err: %v, sql: %s", err, query)
	}
	return nil
}

// deleteEvent 单条删除事件
func (output *ManticoreOutput) deleteEvent(tx *sql.Tx, event *input.Event) error {
	if event == nil || len(event.BeforeRow) == 0 {
		return nil
	}

	// 构建最终 SQL
	query := fmt.Sprintf("DELETE FROM %s WHERE id = %v", event.Table, event.BeforeRow[event.ColumnPkeys[0]])
	// 执行删除
	_, err := tx.Exec(query)
	return err
}
func (output *ManticoreOutput) bulk(events []*input.Event) error {
	if events == nil {
		return nil
	}
	ndjson := make([]string, 0)
	for _, event := range events {
		switch event.Action {
		case "update":
			afterMap := event.ToAfterMap()
			id := afterMap["id"]
			delete(afterMap, "id")
			for key := range afterMap {
				if output.tableColumnType[event.Table][key] == "text" {
					delete(afterMap, key)
				}
			}
			dataMap := map[string]any{
				"id":    id,
				"table": event.Table,
				"doc":   afterMap,
			}
			if event.Database != "" {
				dataMap["cluster"] = event.Database
			}
			d, _ := json.Marshal(map[string]any{event.Action: dataMap})
			ndjson = append(ndjson, string(d))
		case "replace":
			afterMap := event.ToAfterMap()
			id := afterMap["id"]
			delete(afterMap, "id")

			dataMap := map[string]any{
				"id":    id,
				"table": event.Table,
				"doc":   afterMap,
			}
			if event.Database != "" {
				dataMap["cluster"] = event.Database
			}
			d, _ := json.Marshal(map[string]any{event.Action: dataMap})
			ndjson = append(ndjson, string(d))
		case "insert":
			afterMap := event.ToAfterMap()
			id := afterMap["id"]
			delete(afterMap, "id")
			dataMap := map[string]any{
				"id":    id,
				"table": event.Table,
				"doc":   afterMap,
			}
			if event.Database != "" {
				dataMap["cluster"] = event.Database
			}
			d, _ := json.Marshal(map[string]any{event.Action: dataMap})

			ndjson = append(ndjson, string(d))
		case "delete": //{ "delete" : { "table" : "test", "id" : 1 } }
			beforeMap := event.ToBeforeMap()
			dataMap := map[string]any{
				"id":    beforeMap["id"],
				"table": event.Table,
			}
			if event.Database != "" {
				dataMap["cluster"] = event.Database
			}
			d, _ := json.Marshal(map[string]any{event.Action: dataMap})
			ndjson = append(ndjson, string(d))
		}

	}

	resp, err := nhttp.NewRequest().PostNdjson(output.origin+"/bulk", strings.Join(ndjson, "\n"))
	if err != nil {
		return fmt.Errorf("PostNdjson error %v", err)
	}

	resMap := make(map[string]any)
	err = json.Unmarshal([]byte(resp.Body), &resMap)
	if err != nil {
		return fmt.Errorf("json.Unmarshal error %v", err)
	}
	// log.Printf("manticore bulk size: %v\n", len(ndjson))
	errs := resMap["errors"]
	if errs.(bool) {
		line := ntype.ToInt(resMap["current_line"])
		return fmt.Errorf("bulk request=%v\n; response error %v", ndjson[line-1], string(resp.Body))
	}
	return nil
}

// queryByIds 查询多个 id 并返回以 id 字符串为键的文档映射
func (output *ManticoreOutput) queryByIds(beferIds []any, table string) (map[string]map[string]any, error) {
	if len(beferIds) == 0 {
		return nil, nil
	}

	client := &http.Client{}
	body := fmt.Sprintf(`{"table":"%v","query": {"in": {"id": [%s]}}}`, table, ntype.JoinAny(beferIds, ","))

	req, err := http.NewRequest("POST", output.origin+"/search", strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Add("Content-Type", "application/x-ndjson")

	response, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if response != nil {
		defer response.Body.Close()
	}
	res, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	resMap := make(map[string]any)
	err = json.Unmarshal(res, &resMap)
	if err != nil {
		return nil, fmt.Errorf("json.Unmarshal error %v", err)
	}

	errs := resMap["error"]
	if errs != nil {
		return nil, errors.New(string(res))
	}
	hitsAny, ok := resMap["hits"]
	if !ok || hitsAny == nil {
		return nil, nil
	}
	hits := hitsAny.(map[string]any)
	totalFloat, ok := hits["total"].(float64)
	if !ok || int(totalFloat) == 0 {
		return nil, nil
	}
	hitsArray := hits["hits"].([]any)

	result := make(map[string]map[string]any, len(hitsArray))
	for _, h := range hitsArray {
		hit, ok := h.(map[string]any)
		if !ok {
			continue
		}

		src, ok := hit["_source"].(map[string]any)
		if !ok {
			src = map[string]any{}
		}

		// 返回数据里 _source 可能没有 id，兼容 dwid 和顶层 _id。
		idVal, exists := src["id"]
		if !exists || idVal == nil {
			if dwid, hasDWID := src["dwid"]; hasDWID && dwid != nil {
				idVal = dwid
			} else if hid, hasHitID := hit["_id"]; hasHitID && hid != nil {
				idVal = hid
			}
		}
		if idVal == nil {
			continue
		}

		idKey := fmt.Sprintf("%v", idVal)
		src["id"] = idVal
		result[idKey] = src
	}
	return result, nil
}

func setDefault(event *input.Event) {
	for i := 0; i < len(event.AfterRow); i++ {
		if constant.IsDateType(event.ColumnTypes[i]) {
			valus := event.AfterRow[i]
			if valus == nil {
				valus = "1970-01-01T00:00:00"
			}
			t := ntime.Parse(ntype.ToString(valus))
			if t.IsZero() {
				event.AfterRow[i] = 0
			} else {
				event.AfterRow[i] = t.Unix()
			}
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
			t := ntime.Parse(ntype.ToString(valus))
			if t.IsZero() {
				event.BeforeRow[i] = 0
			} else {
				event.BeforeRow[i] = t.Unix()
			}
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

func resetAction(event *input.Event, output *ManticoreOutput) {
	if event.Action != "update" {
		return
	}
	// if event.Action == "update" {
	// 	event.Action = "replace"
	// 	return
	// }
	// 如果修改了主键，action设置为repkey
	if event.BeforeRow[event.ColumnPkeys[0]] != event.AfterRow[event.ColumnPkeys[0]] {
		event.Action = "replace"
		return
	}

	columnType := output.tableColumnType[event.Table]
	if columnType == nil {
		return
	}
	// 如果修改了索引字段，action设置为replace
	columnKey := event.GetKeysByColumns()
	for k, v := range columnType {
		if v != "text" {
			continue
		}
		if event.AfterRow[columnKey[k]] != event.BeforeRow[columnKey[k]] {
			event.Action = "replace"
			return
		}
	}
}

// 移除未知字段
func removeUnknownColumn(event *input.Event, output *ManticoreOutput) {
	unknownColumns := make([]int, 0)

	for n, column := range event.Columns {
		if _, ok := output.tableColumnType[event.Table][column]; !ok {
			unknownColumns = append(unknownColumns, n)
		}
	}
	event.Columns = nslice.Remove(event.Columns, unknownColumns...)
	event.ColumnTypes = nslice.Remove(event.ColumnTypes, unknownColumns...)

	if len(event.BeforeRow) > 0 {
		event.BeforeRow = nslice.Remove(event.BeforeRow, unknownColumns...)
	}
	if len(event.AfterRow) > 0 {
		event.AfterRow = nslice.Remove(event.AfterRow, unknownColumns...)
	}
}

// removeNoChange removes unchanged columns from the event, except the primary key (ID).
func removeNoChange(event *input.Event) {
	if event.Action != "update" || len(event.BeforeRow) != len(event.AfterRow) {
		return
	}

	// Build a set of primary key column indices for O(1) lookup
	pkeySet := make(map[int]struct{})
	for _, idx := range event.ColumnPkeys {
		pkeySet[idx] = struct{}{}
	}

	var unchangedColumns []int

	for i := 0; i < len(event.BeforeRow); i++ {
		// Skip primary key columns
		if _, isPKey := pkeySet[i]; isPKey {
			continue
		}

		before := event.BeforeRow[i]
		after := event.AfterRow[i]

		// Use DeepEqual to safely compare slices, strings, ints, etc.
		if reflect.DeepEqual(before, after) {
			unchangedColumns = append(unchangedColumns, i)
		}
	}

	// Remove unchanged columns from all slices
	event.Columns = nslice.Remove(event.Columns, unchangedColumns...)
	event.ColumnTypes = nslice.Remove(event.ColumnTypes, unchangedColumns...)
	event.BeforeRow = nslice.Remove(event.BeforeRow, unchangedColumns...)
	event.AfterRow = nslice.Remove(event.AfterRow, unchangedColumns...)
	keyMaps := event.GetKeysByColumns()
	event.ColumnPkeys = []int{keyMaps["id"]}
}
