package output

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/go-lazyer/nsync/config"
	"github.com/go-lazyer/nsync/input"
	_ "github.com/go-sql-driver/mysql"
)

type MysqlOutput struct {
	db         *sql.DB
	buffers    []*input.Event //mysql
	bufferTime time.Time      // 每个表的批次时间
	conf       *config.OutputConfig
	mu         sync.Mutex //互斥锁
}

func (mysql *MysqlOutput) NewOutput(cfg *config.OutputConfig) error {
	mysql.conf = cfg
	dsn := fmt.Sprintf("%s:%s@(%s:%v)/%s?charset=utf8mb4&parseTime=True&loc=Local", cfg.Mysql.Username, cfg.Mysql.Password, cfg.Mysql.Host, cfg.Mysql.Port, cfg.Mysql.Database)

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
	mysql.db = db

	// 初始化缓冲区
	mysql.buffers = make([]*input.Event, 0)
	// 初始化缓冲区时间
	mysql.bufferTime = time.Now()

	return nil
}

// 只有保存成功才返回position，
func (mysql *MysqlOutput) Buffer(event *input.Event) (position string, err error) {
	mysql.mu.Lock()
	defer mysql.mu.Unlock()

	mysql.buffers = append(mysql.buffers, event)

	// 触发批量条件
	// 先检查数量触发
	if len(mysql.buffers) >= DefaultMaxBatchSize {
		err = mysql.Flush()
		if err == nil {
			return event.Position, nil
		}
	}
	// 时间触发
	if time.Since(mysql.bufferTime) >= DefaultMaxWaitDuration {
		err = mysql.Flush()
		if err == nil {
			return event.Position, nil
		}
	}
	return "", err
}

func (mysql *MysqlOutput) Flush() error {
	// mysql.mu.Lock()
	// defer mysql.mu.Unlock()

	if len(mysql.buffers) == 0 {
		return nil
	}
	if len(mysql.buffers) == 0 {
		return nil
	}
	tx, err := mysql.db.Begin()
	if err != nil {
		return err
	}

	// 按操作类型分组
	for _, event := range mysql.buffers {
		switch event.Action {
		case "update":
			if err := mysql.updateEvent(tx, event); err != nil {
				tx.Rollback()
				return err
			}
		case "insert":
			if err := mysql.insertEvent(tx, event); err != nil {
				tx.Rollback()
				return err
			}
		case "delete":
			if err := mysql.deleteEvent(tx, event); err != nil {
				tx.Rollback()
				return err
			}
		}

	}
	if err := tx.Commit(); err != nil {
		return err
	}
	// 清空缓冲
	mysql.buffers = mysql.buffers[:0]
	// Reset batch time
	mysql.bufferTime = time.Time{}
	return nil
}

// func (mysql *MysqlOutput) FlushAll() error {
// 	mysql.mu.Lock()
// 	defer mysql.mu.Unlock()

// 	for table := range mysql.buffer {
// 		if err := mysql.Flush(); err != nil {
// 			return err
// 		}
// 	}
// 	return nil
// }

func (mysql *MysqlOutput) Close() error {
	return mysql.db.Close()
}

// updateEvent 单条更新事件
func (mysql *MysqlOutput) updateEvent(tx *sql.Tx, event *input.Event) error {
	if event == nil || len(event.AfterRow) == 0 {
		return nil
	}

	// // 预分配 SQL 和 args 容量
	// var queries []string
	// var allArgs []interface{}

	// // 检查是否有旧值和新值
	// if len(event.Rows) != 2 {
	// 	return fmt.Errorf("update event must have 2 rows: old and new, got %d", len(event.Rows))
	// }

	// // 构建 SET 部分: col1=?, col2=?, ...
	// setParts := make([]string, 0)
	// var args []interface{}
	// for i, col := range event.Columns {
	// 	setParts[i] = fmt.Sprintf("%s=?", quoteIdentifierByMysql(col))
	// 	args = append(args, newRow[i]) // SET 新值
	// }
	// setClause := strings.Join(setParts, ",")

	// // 构建 WHERE 部分: col1=? AND col2=? ...（用旧值）
	// whereParts := make([]string, len(columns))
	// for i := range oldRow {
	// 	whereParts[i] = fmt.Sprintf("%s=?", quoteIdentifierByMysql(columns[i]))
	// 	args = append(args, oldRow[i]) // WHERE 旧值
	// }
	// whereClause := strings.Join(whereParts, " AND ")

	// // 生成单条 UPDATE SQL
	// query := fmt.Sprintf(
	// 	"UPDATE %s SET %s WHERE %s",
	// 	quoteIdentifierByMysql(event.Table),
	// 	setClause,
	// 	whereClause,
	// )

	// queries = append(queries, query)
	// allArgs = append(allArgs, args...)

	// // 在一个事务中顺序执行所有 UPDATE
	// for i, query := range queries {
	// 	start := i * len(event.Columns) * 2 // 简化：每个 query 参数数 = 2 * 列数
	// 	end := start + len(event.Columns)*2
	// 	args := allArgs[start:end]

	// 	_, err := tx.Exec(query, args...)
	// 	if err != nil {
	// 		return fmt.Errorf("exec update failed for event %d: %v", i, err)
	// 	}
	// }

	return nil
}
func (mysql *MysqlOutput) insertEvent(tx *sql.Tx, event *input.Event) error {
	if event == nil || len(event.AfterRow) == 0 {
		return nil
	}
	columns := event.Columns

	// 构建列名部分: `col1`, `col2`, ...
	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = quoteIdentifierByMysql(col)
	}
	columnsClause := strings.Join(quotedColumns, ", ")

	// 用于构建 VALUES 部分
	valuePlaceholders := make([]string, 0) // 每一行的 (?, ?, ...)
	args := make([]interface{}, 0)         // 预分配，避免频繁扩容

	// 为每一行创建 (?, ?, ...)
	placeholders := make([]string, len(event.AfterRow))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	valuePlaceholders = append(valuePlaceholders, "("+strings.Join(placeholders, ",")+")")

	// 将这一行的所有值加入 args
	args = append(args, event.AfterRow...)

	// 如果没有要插入的数据
	if len(valuePlaceholders) == 0 {
		return nil
	}

	// 构建最终 SQL
	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s",
		quoteIdentifierByMysql(event.Table),
		columnsClause,
		strings.Join(valuePlaceholders, ","),
	)

	// 执行
	_, err := tx.Exec(query, args...)
	return err
}

// deleteEvent 单条删除事件
func (mysql *MysqlOutput) deleteEvent(tx *sql.Tx, event *input.Event) error {
	if event == nil || len(event.BeforeRow) == 0 {
		return nil
	}

	columnList := make([]string, 0)
	for i, col := range event.Columns {
		columnList[i] = quoteIdentifierByMysql(col)
	}
	columnClause := strings.Join(columnList, ",")

	// 用于构建 IN 列表: (?,?), (?,?), ...
	valuePlaceholders := make([]string, 0)
	args := make([]interface{}, 0) // 预分配容量

	// 检查每行的列数是否匹配
	// 为每一行生成 (?, ?, ...)
	placeholders := make([]string, len(event.BeforeRow))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	valuePlaceholders = append(valuePlaceholders, "("+strings.Join(placeholders, ",")+")")

	// 将这一行的值加入 args
	args = append(args, event.BeforeRow...)

	// 如果没有要删除的条件
	if len(valuePlaceholders) == 0 {
		return nil
	}

	// 构建最终 SQL
	query := fmt.Sprintf(
		"DELETE FROM %s WHERE (%s) IN (%s)",
		quoteIdentifierByMysql(event.Table),
		columnClause,
		strings.Join(valuePlaceholders, ","),
	)

	// 调试输出
	log.Println("delete query:", query)
	log.Println("delete args count:", len(args))

	// 执行删除
	_, err := tx.Exec(query, args...)
	return err
}
func quoteIdentifierByMysql(name string) string {
	return fmt.Sprintf("`%s`", name)
}
