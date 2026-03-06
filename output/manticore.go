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

// 只有保存成功才返回position，
func (output *ManticoreOutput) Buffer(event *input.Event) (position string, err error) {
	output.mu.Lock()
	defer output.mu.Unlock()

	// //非主键更新
	// if event.Columns[event.ColumnPkeys[0]] != "id" {
	// 	err = output.Flush()
	// 	if err != nil {
	// 		return "", err
	// 	}
	// 	err = output.FlushBySql(event)
	// 	if err != nil {
	// 		return "", err
	// 	}
	// 	return event.Position, nil
	// }

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
func (output *ManticoreOutput) Flush() error {

	if len(output.buffers) == 0 {
		return nil
	}
	//update 会导致二级索引失效，需要转换为replace
	// idmap := make(map[any]struct{})
	for i := 0; i < len(output.buffers); i++ {
		// removeUnknownColumn(output.buffers[i], output)
		//清空没有变化的字段，ID除外
		// removeNoChange(output.buffers[i])

		// setDefault(output.buffers[i])
		// resetAction(output.buffers[i], output)
		//打印table，action。betterevent afterevent
		// log.Printf("flush table:%v;action:%v;\n", output.buffers[i].SourceTable, output.buffers[i].Action)
		// idmap[output.buffers[i].AfterRow[output.buffers[i].ColumnPkeys[0]]] = struct{}{}
	}

	err := output.bulk(output.buffers)
	if err != nil {
		return err
	}

	// 清空缓冲
	output.buffers = output.buffers[:0]
	// Reset batch time
	output.bufferTime = time.Now()
	return nil
}

// func (mysql *ManticoreOutput) FlushAll() error {
// 	mysql.mu.Lock()
// 	defer mysql.mu.Unlock()

// 	for table := range mysql.buffer {
// 		if err := mysql.Flush(); err != nil {
// 			return err
// 		}
// 	}
// 	return nil
// }

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

	client := &http.Client{}
	req, err := http.NewRequest("POST", output.origin+"/bulk", strings.NewReader(strings.Join(ndjson, "\n")))
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/x-ndjson")

	response, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("client.Do error %v", err)
	}
	if response != nil {
		defer response.Body.Close()
	}
	res, err := io.ReadAll(response.Body)
	if err != nil {
		return fmt.Errorf("io.ReadAll error %v", err)
	}

	resMap := make(map[string]any)
	err = json.Unmarshal(res, &resMap)
	if err != nil {
		return fmt.Errorf("json.Unmarshal error %v", err)
	}
	// log.Printf("manticore bulk size: %v\n", len(ndjson))
	errs := resMap["errors"]
	if errs.(bool) {
		line := ntype.ToInt(resMap["current_line"])
		return fmt.Errorf("bulk request=%v\n; response error %v", ndjson[line-1], string(res))
	}
	return nil
}
func (output *ManticoreOutput) searchById(beferId any, table string) (map[string]any, error) {
	client := &http.Client{}
	body := fmt.Sprintf(`{"table":"%v","query": {"equals": {"id": %v}}}`, table, beferId)

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

	// total := jsoniter.Get(res, "hits", "total").ToInt()
	// if total == 0 {
	// 	return nil, nil
	// }

	// m := make(map[string]any, 0)
	// jsoniter.Get(res, "hits", "hits", 0, "_source").ToVal(&m)

	resMap := make(map[string]any)
	err = json.Unmarshal(res, &resMap)
	if err != nil {
		return nil, fmt.Errorf("json.Unmarshal error %v", err)
	}

	errs := resMap["error"]
	if errs != nil {
		return nil, errors.New(string(res))
	}
	hits := resMap["hits"].(map[string]any)
	total := int(hits["total"].(float64))
	if total == 0 {
		return nil, nil
	}
	hitsArray := hits["hits"].([]any)
	firstHit := hitsArray[0].(map[string]any)
	m := firstHit["_source"].(map[string]any)
	return m, nil
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
	event.Columns = nslice.Removes(event.Columns, unknownColumns)
	event.ColumnTypes = nslice.Removes(event.ColumnTypes, unknownColumns)

	if len(event.BeforeRow) > 0 {
		event.BeforeRow = nslice.Removes(event.BeforeRow, unknownColumns)
	}
	if len(event.AfterRow) > 0 {
		event.AfterRow = nslice.Removes(event.AfterRow, unknownColumns)
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
	event.Columns = nslice.Removes(event.Columns, unchangedColumns)
	event.ColumnTypes = nslice.Removes(event.ColumnTypes, unchangedColumns)
	event.BeforeRow = nslice.Removes(event.BeforeRow, unchangedColumns)
	event.AfterRow = nslice.Removes(event.AfterRow, unchangedColumns)
	keyMaps := event.GetKeysByColumns()
	event.ColumnPkeys = []int{keyMaps["id"]}
}
