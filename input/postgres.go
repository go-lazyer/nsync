package input

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-lazyer/nsync/config"
	"github.com/go-lazyer/nsync/constant"
	"github.com/go-lazyer/nsync/filter"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

const (
	pgoutputPluginName = "pgoutput"
	primaryKeyFlag     = 1
)

type PostgresInput struct {
	name           string
	positionFile   string
	positionLSN    string
	conn           *pgconn.PgConn
	event          chan *Event
	filter         *filter.Chain
	publication    string
	slot           string
	statusInterval time.Duration
	closeOnce      sync.Once
	syncEventTotal atomic.Int64
}

func NewPostgresInput(cfg *config.InputPostgres, filterChain *filter.Chain, eventBus chan *Event) (*PostgresInput, error) {
	// NewPostgresInput 初始化 PostgreSQL 逻辑复制输入。
	// 主要流程：
	// 1) 校验并补齐 input.postgres 配置；
	// 2) 建立 replication 连接；
	// 3) 确保 publication 与 logical slot 存在；
	// 4) 启动后台协程持续消费 WAL 并转成统一 Event。
	// 返回的 PostgresInput 会立刻开始拉取复制流，无需额外 start 调用。
	if strings.TrimSpace(cfg.Host) == "" {
		return nil, fmt.Errorf("input.postgres.host is empty")
	}
	if cfg.Port == 0 {
		cfg.Port = 5432
	}
	if strings.TrimSpace(cfg.Database) == "" {
		return nil, fmt.Errorf("input.postgres.database is empty")
	}
	if strings.TrimSpace(cfg.Username) == "" {
		return nil, fmt.Errorf("input.postgres.username is empty")
	}

	publication := strings.TrimSpace(cfg.Publication)
	if publication == "" {
		publication = "nsync_publication"
	}
	slot := strings.TrimSpace(cfg.Slot)
	if slot == "" {
		slot = "nsync_slot"
	}
	statusInterval := time.Duration(cfg.StatusIntervalSec) * time.Second
	if statusInterval <= 0 {
		statusInterval = 10 * time.Second
	}

	replConn, err := pgconn.Connect(context.Background(), buildPostgresDSN(cfg, true))
	if err != nil {
		return nil, err
	}

	input := &PostgresInput{
		conn:           replConn,
		event:          eventBus,
		filter:         filterChain,
		publication:    publication,
		slot:           slot,
		statusInterval: statusInterval,
		name:           cfg.Name,
		positionFile:   fmt.Sprintf("_%s%s", cfg.Name, POSITION_FILE_NAME),
	}

	if err := input.ensurePublication(cfg); err != nil {
		_ = replConn.Close(context.Background())
		return nil, err
	}
	if err := input.ensureLogicalSlot(); err != nil {
		_ = replConn.Close(context.Background())
		return nil, err
	}

	go input.consumeReplication()
	return input, nil
}

func (p *PostgresInput) TotalEvent() int64 {
	// TotalEvent 返回累计事件计数：
	// 第一个值是复制流产生的同步事件数；
	// 第二个值是外部 SubmitEvent 注入的手工事件数。
	return p.syncEventTotal.Load()
}

func (p *PostgresInput) Close() {
	p.closeOnce.Do(func() {
		if p.conn != nil {
			_ = p.conn.Close(context.Background())
		}
	})
}

func (p *PostgresInput) SavePosition(position string) error {
	// SavePosition 持久化最新已处理 LSN 到本地文件。
	// 行为约束：
	// 1) 空 position 直接忽略；
	// 2) 与内存中 position 相同则跳过写盘；
	// 3) 只有发生变化才落盘，减少 I/O 放大。
	if position == "" {
		return nil
	}
	if p.positionLSN == "" {
		p.positionLSN = position
		return nil
	}
	if p.positionLSN == position {
		return nil
	}
	p.positionLSN = position
	return os.WriteFile(p.positionFile, []byte(position), 0644)
}

func (p *PostgresInput) ensurePublication(cfg *config.InputPostgres) error {
	// ensurePublication 确保 publication 存在。
	// 如果目标 publication 不存在，则自动创建 FOR ALL TABLES 的 publication。
	// 该步骤使用普通 SQL 连接完成，不走 replication 协议。
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sqlConn, err := pgx.Connect(ctx, buildPostgresDSN(cfg, false))
	if err != nil {
		return err
	}
	defer sqlConn.Close(ctx)

	var exists bool
	if err := sqlConn.QueryRow(ctx, "select exists(select 1 from pg_publication where pubname = $1)", p.publication).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return nil
	}

	_, err = sqlConn.Exec(ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES", quotePgIdentifier(p.publication)))
	return err
}

func (p *PostgresInput) ensureLogicalSlot() error {
	// ensureLogicalSlot 确保逻辑复制槽存在。
	// 使用 pgoutput 插件创建 slot；若 slot 已存在（错误码 42710）则视为成功，
	// 以支持服务重启后复用同一个复制槽继续消费。
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := pglogrepl.CreateReplicationSlot(ctx, p.conn, p.slot, pgoutputPluginName, pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication})
	if err == nil {
		return nil
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "42710" {
		return nil
	}
	return err
}

func (p *PostgresInput) consumeReplication() {
	// consumeReplication 持续消费 PostgreSQL 复制流并转换事件。
	// 关键点：
	// 1) 从持久化 LSN 或当前系统 LSN 起步；
	// 2) 解析 Keepalive 与 XLogData；
	// 3) 定期上报 StandbyStatus，告知服务端已消费位置；
	// 4) 将逻辑变更转换为统一 Event 推入通道。
	// 当连接异常或解析出现不可恢复错误时退出循环，并关闭事件通道。
	ctx := context.Background()
	relationCache := make(map[uint32]*pglogrepl.RelationMessage)

	startLSN, err := p.loadStartLSN()
	if err != nil {
		log.Printf("pgsql input load start lsn failed: %v", err)
		return
	}

	pluginArgs := []string{
		"proto_version '1'",
		fmt.Sprintf("publication_names '%s'", p.publication),
	}
	if err := pglogrepl.StartReplication(ctx, p.conn, p.slot, startLSN, pglogrepl.StartReplicationOptions{PluginArgs: pluginArgs}); err != nil {
		log.Printf("pgsql start replication failed: %v", err)
		return
	}

	lastReceivedLSN := startLSN
	nextStandbyStatusDeadline := time.Now().Add(p.statusInterval)

	for {
		receiveCtx, cancel := context.WithTimeout(ctx, p.statusInterval)
		rawMsg, err := p.conn.ReceiveMessage(receiveCtx)
		cancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || pgconn.Timeout(err) {
				if time.Now().After(nextStandbyStatusDeadline) {
					if err := p.sendStandbyStatus(ctx, lastReceivedLSN); err != nil {
						log.Printf("pgsql standby status update failed: %v", err)
						return
					}
					nextStandbyStatusDeadline = time.Now().Add(p.statusInterval)
				}
				continue
			}
			log.Printf("pgsql receive message failed: %v", err)
			return
		}

		copyData, ok := rawMsg.(*pgproto3.CopyData)
		if !ok || len(copyData.Data) == 0 {
			continue
		}

		switch copyData.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			keepalive, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
			if err != nil {
				log.Printf("pgsql parse keepalive failed: %v", err)
				continue
			}
			if keepalive.ServerWALEnd > lastReceivedLSN {
				lastReceivedLSN = keepalive.ServerWALEnd
			}
			if keepalive.ReplyRequested {
				if err := p.sendStandbyStatus(ctx, lastReceivedLSN); err != nil {
					log.Printf("pgsql reply keepalive failed: %v", err)
					return
				}
				nextStandbyStatusDeadline = time.Now().Add(p.statusInterval)
			}
		case pglogrepl.XLogDataByteID:
			xlogData, err := pglogrepl.ParseXLogData(copyData.Data[1:])
			if err != nil {
				log.Printf("pgsql parse xlog data failed: %v", err)
				continue
			}
			if err := p.handleLogicalMessage(xlogData, relationCache); err != nil {
				log.Printf("pgsql handle logical message failed: %v", err)
				continue
			}
			lastReceivedLSN = xlogData.WALStart + pglogrepl.LSN(len(xlogData.WALData))
			if time.Now().After(nextStandbyStatusDeadline) {
				if err := p.sendStandbyStatus(ctx, lastReceivedLSN); err != nil {
					log.Printf("pgsql standby status update failed: %v", err)
					return
				}
				nextStandbyStatusDeadline = time.Now().Add(p.statusInterval)
			}
		}
	}
}

func (p *PostgresInput) handleLogicalMessage(xlogData pglogrepl.XLogData, relationCache map[uint32]*pglogrepl.RelationMessage) error {
	// handleLogicalMessage 解析单条 WAL 逻辑消息。
	// 处理策略：
	// 1) RelationMessage：更新 relation 元数据缓存；
	// 2) Insert/Update/Delete：根据 relation 缓存构造标准 Event；
	// 3) 未识别或暂不关心的消息类型忽略，不中断主循环。
	msg, err := pglogrepl.Parse(xlogData.WALData)
	if err != nil {
		return err
	}

	switch m := msg.(type) {
	case *pglogrepl.RelationMessage:
		relationCache[m.RelationID] = m
	case *pglogrepl.InsertMessage:
		relation := relationCache[m.RelationID]
		if relation == nil {
			return nil
		}
		p.emitEvent("insert", relation, nil, m.Tuple, xlogData.WALStart)
	case *pglogrepl.UpdateMessage:
		relation := relationCache[m.RelationID]
		if relation == nil {
			return nil
		}
		var before *pglogrepl.TupleData
		if m.OldTuple != nil {
			before = m.OldTuple
		}
		p.emitEvent("update", relation, before, m.NewTuple, xlogData.WALStart)
	case *pglogrepl.DeleteMessage:
		relation := relationCache[m.RelationID]
		if relation == nil {
			return nil
		}
		p.emitEvent("delete", relation, m.OldTuple, nil, xlogData.WALStart)
	}
	return nil
}

func (p *PostgresInput) emitEvent(action string, relation *pglogrepl.RelationMessage, before *pglogrepl.TupleData, after *pglogrepl.TupleData, position pglogrepl.LSN) {
	// emitEvent 将 pgoutput 的关系/元组数据映射为系统统一 Event。
	// 包含以下转换：
	// 1) 应用 filter 做库表路由；
	// 2) 提取列名、主键列索引与列类型；
	// 3) 将 TupleData 转为 before/after 行；
	// 4) 设置 Position 为当前消息 LSN 并入队。
	schema, table, ok := p.filter.Apply(relation.Namespace, relation.RelationName)
	if !ok {
		return
	}

	columns := make([]string, 0, len(relation.Columns))
	columnTypes := make([]string, 0, len(relation.Columns))
	columnPkeys := make([]int, 0)

	for idx, col := range relation.Columns {
		columns = append(columns, col.Name)
		columnTypes = append(columnTypes, mapPostgresOIDToNsyncType(col.DataType))
		if col.Flags&primaryKeyFlag == primaryKeyFlag {
			columnPkeys = append(columnPkeys, idx)
		}
	}

	beforeRow := tupleToRow(before, len(columns))
	afterRow := tupleToRow(after, len(columns))

	event := &Event{
		InputName:      p.name,
		SourceDatabase: relation.Namespace,
		SourceTable:    relation.RelationName,
		Database:       schema,
		Table:          table,
		Columns:        columns,
		ColumnPkeys:    columnPkeys,
		ColumnTypes:    columnTypes,
		Action:         action,
		BeforeRow:      beforeRow,
		AfterRow:       afterRow,
		Position:       position.String(),
	}

	p.syncEventTotal.Add(1)
	if p.event != nil {
		p.event <- event
	}
}

func (p *PostgresInput) loadStartLSN() (pglogrepl.LSN, error) {
	// loadStartLSN 解析复制起点。
	// 优先读取本地持久化位点文件；若无可用位点，则回退到当前数据库系统 LSN。
	// 这样可同时兼顾“断点续传”和“首次启动”两类场景。
	lsnRaw, err := os.ReadFile(p.positionFile)
	if err == nil {
		value := strings.TrimSpace(string(lsnRaw))
		if value != "" {
			lsn, parseErr := pglogrepl.ParseLSN(value)
			if parseErr != nil {
				return 0, parseErr
			}
			p.positionLSN = value
			return lsn, nil
		}
	}

	sysident, err := pglogrepl.IdentifySystem(context.Background(), p.conn)
	if err != nil {
		return 0, err
	}
	p.positionLSN = sysident.XLogPos.String()
	return sysident.XLogPos, nil
}

func (p *PostgresInput) sendStandbyStatus(ctx context.Context, lsn pglogrepl.LSN) error {
	// sendStandbyStatus 向主库回报已写入/已刷盘/已应用的 LSN。
	// 当前实现将三者统一设置为同一个 lsn，语义上表示本端已消费到该位置，
	// 便于服务端推进复制确认点并控制 WAL 保留。
	return pglogrepl.SendStandbyStatusUpdate(ctx, p.conn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: lsn,
		WALFlushPosition: lsn,
		WALApplyPosition: lsn,
		ClientTime:       time.Now(),
	})
}

func mapPostgresOIDToNsyncType(oid uint32) string {
	// mapPostgresOIDToNsyncType 将 PostgreSQL 列 OID 映射为 nsync 内部标准类型。
	// 映射用于下游 output 做类型分支处理；未知 OID 回退到 string，避免中断同步。
	switch oid {
	case pgtype.Int2OID, pgtype.Int4OID, pgtype.BoolOID:
		return constant.COLUMN_TYPE_INT32
	case pgtype.Int8OID:
		return constant.COLUMN_TYPE_INT64
	case pgtype.Float4OID:
		return constant.COLUMN_TYPE_FLOAT32
	case pgtype.Float8OID, pgtype.NumericOID:
		return constant.COLUMN_TYPE_FLOAT64
	case pgtype.TimestampOID, pgtype.TimestamptzOID:
		return constant.COLUMN_TYPE_TIMESTAMP
	case pgtype.DateOID:
		return constant.COLUMN_TYPE_DATE
	case pgtype.TimeOID:
		return constant.COLUMN_TYPE_TIME
	case pgtype.JSONOID, pgtype.JSONBOID:
		return constant.COLUMN_TYPE_JSON
	case pgtype.TextOID, pgtype.VarcharOID, pgtype.BPCharOID, pgtype.UUIDOID:
		return constant.COLUMN_TYPE_STRING
	case pgtype.Int2ArrayOID, pgtype.Int4ArrayOID:
		return constant.COLUMN_TYPE_INT32ARR
	case pgtype.Int8ArrayOID:
		return constant.COLUMN_TYPE_INT64ARR
	case pgtype.TextArrayOID, pgtype.VarcharArrayOID:
		return constant.COLUMN_TYPE_STRINGARR
	default:
		return constant.COLUMN_TYPE_STRING
	}
}

func tupleToRow(tuple *pglogrepl.TupleData, size int) []any {
	// tupleToRow 将 pgoutput 的 TupleData 变为 []any 行数据。
	// 数据类型处理：
	// 1) 'n'/'u' 记为 nil（NULL 或 TOAST 未变化）；
	// 2) 't'/'b' 统一转字符串，保证事件结构稳定；
	// 3) 未知类型安全降级为 nil。
	// size 用于约束输出长度，防止异常数据越界。
	if tuple == nil {
		return nil
	}
	row := make([]any, size)
	for idx, col := range tuple.Columns {
		if idx >= size {
			break
		}
		switch col.DataType {
		case 'n', 'u':
			row[idx] = nil
		case 't', 'b':
			row[idx] = string(col.Data)
		default:
			row[idx] = nil
		}
	}
	return row
}

func quotePgIdentifier(name string) string {
	// quotePgIdentifier 安全引用 PostgreSQL 标识符。
	// 会把内部双引号转义为两个双引号，避免注入和关键字冲突导致 SQL 失败。
	escaped := strings.ReplaceAll(name, "\"", "\"\"")
	return fmt.Sprintf("\"%s\"", escaped)
}

func buildPostgresDSN(cfg *config.InputPostgres, replication bool) string {
	// buildPostgresDSN 构造 PostgreSQL 连接串。
	// replication=true 时追加 replication=database，供逻辑复制连接使用；
	// 否则为普通 SQL 连接（用于检查 publication 等元数据）。
	query := "sslmode=disable"
	if replication {
		query += "&replication=database"
	}
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?%s",
		url.QueryEscape(cfg.Username),
		url.QueryEscape(cfg.Password),
		cfg.Host,
		cfg.Port,
		url.PathEscape(cfg.Database),
		query,
	)
}
