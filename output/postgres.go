package output

import (
	"context"
	"fmt"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/go-lazyer/nsync/config"
	"github.com/go-lazyer/nsync/input"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresOutput struct {
	db         *pgxpool.Pool
	buffers    []*input.Event
	bufferTime time.Time
	conf       *config.OutputConfig
	mu         sync.Mutex
	batchSize  int
}

func (output *PostgresOutput) NewOutput(cfg *config.OutputConfig) error {
	output.conf = cfg
	output.batchSize = cfg.Postgres.BatchSize
	if output.batchSize <= 0 {
		output.batchSize = DefaultMaxBatchSize
	}

	dsn := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=disable",
		url.QueryEscape(cfg.Postgres.Username),
		url.QueryEscape(cfg.Postgres.Password),
		cfg.Postgres.Host,
		cfg.Postgres.Port,
		cfg.Postgres.Database,
	)

	db, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		return err
	}

	if err := db.Ping(context.Background()); err != nil {
		return err
	}

	output.db = db
	output.buffers = make([]*input.Event, 0)
	output.bufferTime = time.Now()

	return nil
}
func (output *PostgresOutput) CountBufferEvent() int {
	return len(output.buffers)
}
func (output *PostgresOutput) Buffer(event *input.Event) (map[string]string, error) {
	output.mu.Lock()
	output.buffers = append(output.buffers, event)
	output.mu.Unlock()

	return output.Flush(false)
}

func (output *PostgresOutput) Flush(force bool) (map[string]string, error) {
	output.mu.Lock()
	defer output.mu.Unlock()

	if len(output.buffers) == 0 {
		return nil, nil
	}

	if !force && len(output.buffers) < output.batchSize && time.Since(output.bufferTime) < DefaultMaxWaitDuration {
		return nil, nil
	}

	ctx := context.Background()
	positionMap := GetPosition(output.buffers)

	for _, event := range output.buffers {
		if event == nil {
			continue
		}

		switch strings.ToLower(event.Action) {
		case "update":
			if err := output.updateEvent(ctx, output.db, event); err != nil {
				return nil, wrapPostgresFlushEventError("update", event, err)
			}
		case "insert", "replace":
			if err := output.insertEvent(ctx, output.db, event); err != nil {
				return nil, wrapPostgresFlushEventError("insert", event, err)
			}
		case "delete":
			if err := output.deleteEvent(ctx, output.db, event); err != nil {
				return nil, wrapPostgresFlushEventError("delete", event, err)
			}
		}
	}

	output.buffers = output.buffers[:0]
	output.bufferTime = time.Now()

	return positionMap, nil
}

func wrapPostgresFlushEventError(operation string, event *input.Event, err error) error {
	if event == nil {
		return fmt.Errorf("postgres %s failed: %w", operation, err)
	}

	return fmt.Errorf(
		"postgres %s failed: table=%s action=%s position=%s event=%+v err=%w",
		operation,
		event.Table,
		event.Action,
		event.Position,
		event,
		err,
	)
}

func (output *PostgresOutput) insertEvent(ctx context.Context, exec *pgxpool.Pool, event *input.Event) error {
	if event == nil || len(event.AfterRow) == 0 || len(event.Columns) == 0 {
		return nil
	}
	if strings.TrimSpace(event.Table) == "" {
		return fmt.Errorf("postgres insert failed: empty table")
	}

	if len(event.Columns) != len(event.AfterRow) {
		return fmt.Errorf("postgres insert failed: columns(%d) != values(%d)", len(event.Columns), len(event.AfterRow))
	}

	quotedColumns := make([]string, len(event.Columns))
	placeholders := make([]string, len(event.Columns))
	args := make([]any, len(event.AfterRow))

	for i, col := range event.Columns {
		quotedColumns[i] = quoteIdentifierByPostgres(col)
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = event.AfterRow[i]
	}
	tableName := quoteTableNameByPostgres(event.Table)

	conflictColumns := make([]string, 0, len(event.ColumnPkeys))
	pkeySet := make(map[int]struct{}, len(event.ColumnPkeys))
	for _, index := range event.ColumnPkeys {
		if index < 0 || index >= len(event.Columns) {
			return fmt.Errorf("postgres insert failed: invalid primary key index %d", index)
		}
		pkeySet[index] = struct{}{}
		conflictColumns = append(conflictColumns, quoteIdentifierByPostgres(event.Columns[index]))
	}

	updateParts := make([]string, 0, len(event.Columns))
	updateWhenChangedParts := make([]string, 0, len(event.Columns))
	for i, col := range event.Columns {
		if _, isPkey := pkeySet[i]; isPkey {
			continue
		}

		quotedCol := quoteIdentifierByPostgres(col)
		updateParts = append(updateParts, fmt.Sprintf(
			"%s = CASE WHEN %s.%s IS DISTINCT FROM EXCLUDED.%s THEN EXCLUDED.%s ELSE %s.%s END",
			quotedCol,
			tableName,
			quotedCol,
			quotedCol,
			quotedCol,
			tableName,
			quotedCol,
		))
		updateWhenChangedParts = append(updateWhenChangedParts, fmt.Sprintf("%s.%s IS DISTINCT FROM EXCLUDED.%s", tableName, quotedCol, quotedCol))
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		tableName,
		strings.Join(quotedColumns, ", "),
		strings.Join(placeholders, ", "),
	)
	if len(conflictColumns) > 0 {
		if len(updateParts) == 0 {
			query += fmt.Sprintf(" ON CONFLICT (%s) DO NOTHING", strings.Join(conflictColumns, ", "))
		} else {
			query += fmt.Sprintf(
				" ON CONFLICT (%s) DO UPDATE SET %s WHERE %s",
				strings.Join(conflictColumns, ", "),
				strings.Join(updateParts, ", "),
				strings.Join(updateWhenChangedParts, " OR "),
			)
		}
	}
	_, err := exec.Exec(ctx, query, args...)
	return err
}

func (output *PostgresOutput) updateEvent(ctx context.Context, exec *pgxpool.Pool, event *input.Event) error {

	if event == nil || len(event.Columns) == 0 || len(event.AfterRow) == 0 {
		return nil
	}
	if strings.TrimSpace(event.Table) == "" {
		return fmt.Errorf("postgres update failed: empty table")
	}

	if len(event.Columns) != len(event.AfterRow) {
		return fmt.Errorf("postgres update failed: columns(%d) != after values(%d)", len(event.Columns), len(event.AfterRow))
	}
	if len(event.BeforeRow) > 0 && len(event.Columns) != len(event.BeforeRow) {
		return fmt.Errorf("postgres update failed: columns(%d) != before values(%d)", len(event.Columns), len(event.BeforeRow))
	}
	if len(event.ColumnPkeys) == 0 {
		return fmt.Errorf("postgres update failed: missing primary key columns")
	}

	setParts := make([]string, 0, len(event.Columns))
	args := make([]any, 0, len(event.Columns)+len(event.ColumnPkeys))

	for i, col := range event.Columns {
		if len(event.BeforeRow) > 0 && reflect.DeepEqual(event.BeforeRow[i], event.AfterRow[i]) {
			continue
		}

		setParts = append(setParts, fmt.Sprintf("%s = $%d", quoteIdentifierByPostgres(col), len(args)+1))
		args = append(args, event.AfterRow[i])
	}

	if len(setParts) == 0 {
		return nil
	}

	whereValues := event.BeforeRow
	if len(whereValues) == 0 {
		whereValues = event.AfterRow
	}

	whereParts := make([]string, 0, len(event.ColumnPkeys))
	for _, index := range event.ColumnPkeys {
		if index < 0 || index >= len(event.Columns) || index >= len(whereValues) {
			return fmt.Errorf("postgres update failed: invalid primary key index %d", index)
		}

		argIndex := len(args) + 1
		whereParts = append(whereParts, fmt.Sprintf("%s = $%d", quoteIdentifierByPostgres(event.Columns[index]), argIndex))
		args = append(args, whereValues[index])
	}

	query := fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s",
		quoteTableNameByPostgres(event.Table),
		strings.Join(setParts, ", "),
		strings.Join(whereParts, " AND "),
	)

	_, err := exec.Exec(ctx, query, args...)
	return err
}

func (output *PostgresOutput) deleteEvent(ctx context.Context, exec *pgxpool.Pool, event *input.Event) error {
	if event == nil || len(event.Columns) == 0 {
		return nil
	}
	if strings.TrimSpace(event.Table) == "" {
		return fmt.Errorf("postgres delete failed: empty table")
	}
	if len(event.ColumnPkeys) == 0 {
		return fmt.Errorf("postgres delete failed: missing primary key columns")
	}

	whereValues := event.BeforeRow
	if len(whereValues) == 0 {
		whereValues = event.AfterRow
	}
	if len(whereValues) == 0 {
		return nil
	}
	if len(event.Columns) != len(whereValues) {
		return fmt.Errorf("postgres delete failed: columns(%d) != where values(%d)", len(event.Columns), len(whereValues))
	}

	whereParts := make([]string, 0, len(event.ColumnPkeys))
	args := make([]any, 0, len(event.ColumnPkeys))

	for _, index := range event.ColumnPkeys {
		if index < 0 || index >= len(event.Columns) || index >= len(whereValues) {
			return fmt.Errorf("postgres delete failed: invalid primary key index %d", index)
		}

		argIndex := len(args) + 1
		whereParts = append(whereParts, fmt.Sprintf("%s = $%d", quoteIdentifierByPostgres(event.Columns[index]), argIndex))
		args = append(args, whereValues[index])
	}

	query := fmt.Sprintf(
		"DELETE FROM %s WHERE %s",
		quoteTableNameByPostgres(event.Table),
		strings.Join(whereParts, " AND "),
	)

	_, err := exec.Exec(ctx, query, args...)
	return err
}

// quoteIdentifierByPostgres 对单个 SQL 标识符加双引号并转义内部引号，避免关键字/特殊字符导致 SQL 语法错误。
func quoteIdentifierByPostgres(name string) string {
	escaped := strings.ReplaceAll(name, "\"", "\"\"")
	return fmt.Sprintf("\"%s\"", escaped)
}

// quoteTableNameByPostgres 处理 schema.table 形式的表名，逐段安全引用后再拼接。
func quoteTableNameByPostgres(name string) string {
	parts := strings.Split(name, ".")
	quoted := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		quoted = append(quoted, quoteIdentifierByPostgres(trimmed))
	}
	if len(quoted) == 0 {
		return quoteIdentifierByPostgres(name)
	}
	return strings.Join(quoted, ".")
}
