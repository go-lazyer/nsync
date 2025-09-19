package output

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-lazyer/nsync/config"
	"github.com/go-lazyer/nsync/input"
)

type PostgresqlOutput struct {
	db              *sql.DB
	buffer          map[string][]*input.Event
	MaxBatchSize    int
	MaxWaitDuration time.Duration

	conf    *config.OutputConfig
	batches map[string]time.Time

	mu sync.Mutex
}

func (pgsql *PostgresqlOutput) NewOutput(cfg *config.OutputConfig) error {
	pgsql.conf = cfg
	//postgres://user:pass@host/db
	dsn := fmt.Sprintf("postgres://%v:%v@%v/%v?sslmode=disable&timezone=Asia/Shanghai", cfg.Postgresql.Username, cfg.Postgresql.Password, cfg.Postgresql.Host, cfg.Postgresql.Database)

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return err
	}
	err = db.Ping()
	if err != nil {
		return err
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)
	pgsql.db = db
	return nil
}

func (p *PostgresqlOutput) Buffer(event *input.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	key := fmt.Sprintf("%s.%s", event.Schema, event.Table)
	p.buffer[key] = append(p.buffer[key], event)

	// 触发批量条件
	if len(p.buffer[key]) >= p.MaxBatchSize {
		go p.Flush(key)
	}

	// 初始化或更新批次时间
	if _, exists := p.batches[key]; !exists {
		p.batches[key] = time.Now()
	}

	// 时间窗口触发
	if time.Since(p.batches[key]) >= p.MaxWaitDuration {
		go p.Flush(key)
	}

	return nil
}

func (p *PostgresqlOutput) Flush(table string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	events, exists := p.buffer[table]
	if !exists || len(events) == 0 {
		return nil
	}
	tx, err := p.db.Begin()
	if err != nil {
		return err
	}

	// 按操作类型分组
	for _, event := range events {
		switch event.Action {
		case "insert":
			if err := p.bulkInsert(tx, events); err != nil {
				tx.Rollback()
				return err
			}
		case "update":
			// if err := p.bulkUpdate(tx, group); err != nil {
			// 	tx.Rollback()
			// 	return err
			// }
		case "delete":
			// if err := p.bulkDelete(tx, group); err != nil {
			// 	tx.Rollback()
			// 	return err
			// }
		}

	}
	if err := tx.Commit(); err != nil {
		return err
	}
	// 清空缓冲
	delete(p.buffer, table)
	delete(p.batches, table)

	return nil
}

func (p *PostgresqlOutput) FlushAll() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for table := range p.buffer {
		if err := p.Flush(table); err != nil {
			return err
		}
	}
	return nil
}

func (p *PostgresqlOutput) Close() error {
	return p.db.Close()
}

// 批量插入实现
func (p *PostgresqlOutput) bulkInsert(tx *sql.Tx, events []*input.Event) error {
	if len(events) == 0 {
		return nil
	}

	table := events[0].Table
	columns := make([]string, 0)
	// for i, col := range table.Columns {
	// 	columns[i] = quoteIdentifierByPgsql(col.Name)
	// }

	valueArgs := make([]string, 0, len(events))
	valueParams := make([]interface{}, 0, len(events)*len(columns))

	counter := 1
	for _, event := range events {
		params := make([]string, len(columns))
		for i := range params {
			params[i] = fmt.Sprintf("$%d", counter)
			counter++
		}
		valueArgs = append(valueArgs, "("+strings.Join(params, ",")+")")
		valueParams = append(valueParams, event.Rows[0]...)
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s",
		quoteIdentifierByPgsql(table),
		strings.Join(columns, ","),
		strings.Join(valueArgs, ","),
	)
	_, err := tx.Exec(query, valueParams...)
	return err
}

func (p *PostgresqlOutput) insertEvent(tx *sql.Tx, event *input.Event) error {
	if event == nil || len(event.Rows) == 0 {
		return nil
	}

	// 生成列名列表
	columns := make([]string, len(event.Rows))
	for i, col := range event.Columns {
		columns[i] = quoteIdentifierByPgsql(col)
	}

	// 准备参数占位符
	params := make([]string, len(columns))
	for i := range params {
		params[i] = fmt.Sprintf("$%d", i+1) // 每个插入语句的参数独立从$1开始
	}

	// 构建完整SQL
	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		fmt.Sprintf(`"%s"`, event.Table),
		strings.Join(columns, ","),
		strings.Join(params, ","),
	)

	// 执行插入
	_, err := tx.Exec(query, event.Rows[0]...)
	return err
}

func quoteIdentifierByPgsql(name string) string {
	return fmt.Sprintf(`"%s"`, name)
}
