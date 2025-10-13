package output

import (
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

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
	buffers         []*input.Event //mysql
	bufferTime      time.Time      // 每个表的批次时间
	conf            *config.OutputConfig
	tableColumnType map[string]map[string]string
	mu              sync.Mutex //互斥锁
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

	// 初始化缓冲区
	output.buffers = make([]*input.Event, 0)
	// 初始化缓冲区时间
	output.bufferTime = time.Now()

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

	output.buffers = append(output.buffers, event)

	// 触发批量条件
	// 先检查数量触发
	if len(output.buffers) >= DefaultMaxBatchSize {
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

func (output *ManticoreOutput) Flush() error {

	if len(output.buffers) == 0 {
		return nil
	}
	tx, err := output.db.Begin()
	if err != nil {
		return err
	}
	// UPDATE 更新全文字段或列存属性的内容请使用 REPLACE。
	// UPDATE 主键，需要拆分为delete加insert
	for _, event := range output.buffers {
		removeUnknownColumn(event, output)
		setDefault(event)
		resetAction(event, output)
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
		case "repkey": // 修改主键，先replace新的，然后删除旧的
			if err := output.replaceEvent(tx, event); err != nil {
				tx.Rollback()
				return err
			}
			if err := output.deleteEvent(tx, event); err != nil {
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

	}
	if err := tx.Commit(); err != nil {
		return err
	}
	// 清空缓冲
	output.buffers = output.buffers[:0]
	// Reset batch time
	output.bufferTime = time.Time{}
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
	query := fmt.Sprintf("update %s set %s  where %v=%v", event.Table, strings.Join(sets, ","), event.Columns[event.ColumnPkeys[0]], event.AfterRow[event.ColumnPkeys[0]])

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
func setDefault(event *input.Event) {
	for i := 0; i < len(event.AfterRow); i++ {
		if constant.IsDateType(event.ColumnTypes[i]) {
			valus := event.AfterRow[i]
			if valus == nil {
				valus = "1970-01-01T00:00:00"
			}
			event.AfterRow[i] = ntime.Parse(ntype.ToString(valus)).Unix()
		} else if constant.IsStringType(event.ColumnTypes[i]) {
			valus := event.AfterRow[i]
			if valus == nil {
				valus = ""
			}
			if ntype.IsSlice(valus) {
				valus = string(event.AfterRow[i].([]uint8))
			}
			event.AfterRow[i] = CleanSpecialChars(valus)
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
			valus := event.BeforeRow[i]
			if valus == nil {
				valus = ""
			}
			if ntype.IsSlice(valus) {
				valus = string(event.BeforeRow[i].([]uint8))
			}
			event.BeforeRow[i] = CleanSpecialChars(valus)
		} else {
			if event.BeforeRow[i] == nil {
				event.BeforeRow[i] = 0
			}
		}
	}
}

// 如果修改了主键，action设置为repkey
// 如果修改了索引字段，action设置为replace
func resetAction(event *input.Event, output *ManticoreOutput) {
	if event.Action != "update" {
		return
	}
	// 如果修改了主键，action设置为repkey
	if event.AfterRow[event.ColumnPkeys[0]] != event.BeforeRow[event.ColumnPkeys[0]] {
		event.Action = "repkey"
		return
	}
	columnType := output.tableColumnType[event.Table]
	if columnType == nil {
		return
	}
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
	if event.Action != "update" {
		return
	}

	columns := event.Columns

	for n, column := range columns {
		if _, ok := output.tableColumnType[event.Table][column]; ok {
			continue
		}
		event.Columns = append(columns[:n], columns[n+1:]...)
		event.ColumnTypes = append(event.ColumnTypes[:n], event.ColumnTypes[n+1:]...)
		if len(event.BeforeRow) > 0 {
			event.BeforeRow = append(event.BeforeRow[:n], event.BeforeRow[n+1:]...)
		}
		if len(event.AfterRow) > 0 {
			event.AfterRow = append(event.AfterRow[:n], event.AfterRow[n+1:]...)
		}
	}
}
