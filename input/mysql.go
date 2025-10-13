package input

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"log"

	"github.com/go-lazyer/nsync/config"
	"github.com/go-lazyer/nsync/constant"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

const POSITION_FILE_NAME = "_mysql_binlog_gtid_position.info"

type MysqlInput struct {
	canal *canal.Canal
	event chan *Event
	once  sync.Once
}

func NewMysqlInput(cfg *config.InputConfig) (*MysqlInput, error) {
	// 1. 初始化 Canal 配置
	c, err := canal.NewCanal(&canal.Config{
		ServerID: 128, // 必须设置有效 ServerID
		Addr:     cfg.Mysql.Host + ":" + strconv.Itoa(cfg.Mysql.Port),
		User:     cfg.Mysql.Username,
		Password: cfg.Mysql.Password,
		Flavor:   "mysql", // 明确指定数据库类型
	})
	if err != nil {
		return nil, err
	}
	mysqlInput := &MysqlInput{
		canal: c,
		event: make(chan *Event, 1024),
	}
	handler := &EventHandler{
		input: mysqlInput,
	}
	c.SetEventHandler(handler)
	gtid, err := os.ReadFile(POSITION_FILE_NAME)
	if err != nil {
		log.Printf("Failed to read GTID file: %v", err)
	}

	var gs mysql.GTIDSet
	if string(gtid) != "" {
		if gs, err = mysql.ParseGTIDSet("mysql", string(gtid)); err != nil {
			return nil, err
		}
	} else {
		if gs, err = c.GetMasterGTIDSet(); err != nil {
			log.Fatal(err)
		}
		if gs.String() == "" {
			log.Fatal("the gtid value is empty, please confirm whether to enable gtid!")
		}
		// mysqlPos.BinlogGTID = gs.String()
		// if err := mysqlPos.SavePosition(); err != nil {
		// 	log.Fatal(err)
		// }
	}

	// 3. 注册事件处理器
	// 4. 启动 Canal
	go func() {
		if gs == nil {
			err = c.StartFromGTID(gs) // 默认从第一个binlog文件开始
		} else {
			err = c.StartFromGTID(gs)
		}
		if err != nil {
			fmt.Printf("mysql canal run error:%v\n", err)
			close(mysqlInput.event)
		}
	}()
	return mysqlInput, nil
}

func (m *MysqlInput) GetEvent() <-chan *Event {
	return m.event
}
func (i *MysqlInput) CloseEvent() {
	i.once.Do(func() {
		close(i.event)
	})
}
func (i *MysqlInput) SavePosition(position string) error {
	position = strings.ReplaceAll(position, ":", ":1-")
	return os.WriteFile(POSITION_FILE_NAME, []byte(position), 0644)
}

// *OnRow(e RowsEvent)
// 📌 核心方法：处理数据变更事件（INSERT/UPDATE/DELETE）
// ⚡️ 触发场景：每当有DML操作时，携带变更前后的行数据（e.Rows）

// OnDDL(...)
// 📌 关键方法：处理DDL语句（CREATE/ALTER/DROP等）
// ⚡️ 参数亮点：

// queryEvent: 含原始SQL语句（string(queryEvent.Query)）
// nextPos: 记录该DDL操作后的binlog位置
// OnRowsQueryEvent(...)
// 🔍 高级用法：获取DML操作的原始SQL（需开启binlog_rows_query_log_events）
// 💡 典型用途：审计日志、SQL解析（可获取包含注释的完整SQL）

// OnXID(...)
// 🔄 事务回调：在事务提交时触发（携带事务结束的binlog位置）
// 📍 典型用途：事务级数据一致性控制

// OnGTID(...)
// 🌐 复制控制：处理GTID事件（主从复制场景）
// 📦 参数结构：gtidEvent 包含 GTID 详细信息

// OnPosSynced(...)
// 💾 持久化钩子：在binlog位置同步时触发
// ⚠️ 注意：当force=true时需要立即持久化位置信息

// OnRotate(...)
// 🔧 维护事件：处理binlog文件切换（如mysql-bin.000001 → mysql-bin.000002）
// 🛠️ 典型用途：维护binlog位置跟踪

// String()
// 🏷️ 标识方法：返回处理器名称（用于日志区分不同处理器）

type EventHandler struct {
	canal.DummyEventHandler
	input      *MysqlInput
	binlogGTID string
}

// 📌 核心方法：处理数据变更事件（INSERT/UPDATE/DELETE）
// ⚡️ 触发场景：每当有DML操作时，携带变更前后的行数据（e.Rows）
func (h *EventHandler) OnRow(e *canal.RowsEvent) error {
	columns := make([]string, 0)
	types := make([]string, 0)
	for _, column := range e.Table.Columns {
		columns = append(columns, column.Name)
		t := strings.Split(column.RawType, "(")[0]
		if _, ok := constant.ColumnTypes[t]; ok {
			types = append(types, t)
		} else {
			types = append(types, "varchar")
		}
	}
	beforeRows := make([][]any, 0)
	afterRows := make([][]any, 0)

	var count int = 0
	switch e.Action {
	case canal.UpdateAction:
		for n, row := range e.Rows {
			if n%2 == 0 {
				beforeRows = append(beforeRows, row)
			} else {
				afterRows = append(afterRows, row)
			}
		}
		count = len(e.Rows) / 2
	case canal.InsertAction:
		afterRows = e.Rows
		count = len(e.Rows)
	default:
		beforeRows = e.Rows
		count = len(e.Rows)
	}

	for i := 0; i < count; i++ {
		event := &Event{
			Schema:       e.Table.Schema,
			Table:        e.Table.Name,
			SourceSchema: e.Table.Schema,
			SourceTable:  e.Table.Name,
			Action:       e.Action,
			Position:     h.binlogGTID,
			Columns:      columns,
			ColumnPkeys:  e.Table.PKColumns,
			ColumnTypes:  types,
		}
		if len(beforeRows) > i {
			event.BeforeRow = beforeRows[i]
		}
		if len(afterRows) > i {
			event.AfterRow = afterRows[i]
		}

		// 发送到通道（带上下文超时控制）
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		select {
		case h.input.event <- event:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

// OnTableChanged(...)
// 🧹 缓存管理：表结构变更时触发（先于OnDDL）
// 🔨 必做操作：清理该表关联的缓存/元数据
func (h *EventHandler) OnTableChanged(header *replication.EventHeader, schema string, table string) error {
	fmt.Printf("schema=%v,table=%v,%v\n", schema, table, header)
	return nil
}

// 🌐 复制控制：处理GTID事件（主从复制场景）
// 📦 参数结构：gtidEvent 包含 GTID 详细信息
func (h *EventHandler) OnGTID(header *replication.EventHeader, gtidEvent mysql.BinlogGTIDEvent) error {
	gtid, err := gtidEvent.GTIDNext()
	if err != nil {
		return err
	}
	h.binlogGTID = gtid.String()
	return nil
}

// 💾 持久化钩子：在binlog位置同步时触发
// ⚠️ 注意：当force=true时需要立即持久化位置信息
func (h *EventHandler) OnPosSynced(header *replication.EventHeader, pos mysql.Position, set mysql.GTIDSet, force bool) error {
	if set == nil {
		return nil
	}
	// h.input.binlogName = pos.Name
	// h.input.binlogPos = pos.Pos
	// h.input.binlogGTID = set.String()
	// log.Printf("Position synced: %s, GTID: %v", set.String(), force)
	return nil
}
func (h *EventHandler) String() string {
	return "MySQLInputEventHandler"
}
