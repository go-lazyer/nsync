package input

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"log"

	"github.com/go-lazyer/nsync/config"
	"github.com/go-lazyer/nsync/constant"
	"github.com/go-lazyer/nsync/filter"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

type MysqlInput struct {
	name         string
	positionFile string
	positionGTID string
	canal        *canal.Canal
	event        chan *Event
	once         sync.Once
	filter       *filter.Chain
	eventTotal   atomic.Int64 // 统计通过同步接口添加的事件数量
}

func (i *MysqlInput) Close() {
	if i == nil {
		return
	}
	i.once.Do(func() {
		if i.canal != nil {
			i.canal.Close()
		}
	})
}

func (i *MysqlInput) TotalEvent() int64 {
	if i == nil {
		return 0
	}
	return i.eventTotal.Load()
}

func NewMysqlInput(cfg *config.InputMysql, filter *filter.Chain, event chan *Event) (*MysqlInput, error) {
	cf := canal.NewDefaultConfig()
	cf.Addr = cfg.Host + ":" + strconv.Itoa(cfg.Port)
	cf.User = cfg.Username
	cf.Password = cfg.Password
	cf.Dump.ExecutionPath = "" // ignore mysqldump, use binlog only

	c, err := canal.NewCanal(cf)
	if err != nil {
		return nil, err
	}
	mysqlInput := &MysqlInput{
		canal:        c,
		event:        event,
		filter:       filter,
		name:         cfg.Name,
		positionFile: fmt.Sprintf("_%s%s", cfg.Name, POSITION_FILE_NAME),
	}
	handler := &EventHandler{
		input: mysqlInput,
	}
	c.SetEventHandler(handler)
	gtid, err := os.ReadFile(mysqlInput.positionFile)
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
			panic(fmt.Sprintf("mysql canal run error:%v\n", err))
		}
	}()
	return mysqlInput, nil
}

func (i *MysqlInput) SavePosition(position string) error {
	if i.positionGTID == "" { //表示系统刚启动,所以第一次不回保存位置
		i.positionGTID = position
		return nil
	}
	if i.positionGTID == position { //事务未完成 内多条语句，位置未变更，
		return nil
	}
	// p := strings.ReplaceAll(i.positionGTID, ":", ":1-")
	i.positionGTID = position
	return os.WriteFile(i.positionFile, []byte(position), 0644)
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
	schema, table, ok := h.input.filter.Apply(e.Table.Schema, e.Table.Name)
	if !ok {
		return nil
	}

	columns := make([]string, len(e.Table.Columns))
	types := make([]string, len(e.Table.Columns))
	for n, column := range e.Table.Columns {
		columns[n] = column.Name
		t := strings.Split(column.RawType, "(")[0]
		if v, ok := constant.MysqlColumnType[t]; ok {
			types[n] = v
		} else {
			types[n] = constant.COLUMN_TYPE_STRING
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
			InputName:      h.input.name,
			SourceDatabase: e.Table.Schema,
			SourceTable:    e.Table.Name,
			Database:       schema,
			Table:          table,
			Action:         e.Action,
			Position:       h.binlogGTID,
			Columns:        append([]string(nil), columns...), // 深拷贝列名切片
			ColumnPkeys:    e.Table.PKColumns,
			ColumnTypes:    append([]string(nil), types...), // 深拷贝列类型切片
		}

		if len(beforeRows) > i {
			event.BeforeRow = beforeRows[i]
		}
		if len(afterRows) > i {
			event.AfterRow = afterRows[i]
		}
		// channel 满时阻塞等待消费者，形成背压，避免丢事件
		if h.input.event != nil {
			h.input.event <- event
			h.input.eventTotal.Add(1)
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
	// log.Printf("OnGTID synced: %s, GTID", gtid.String())
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
	// log.Printf("OnPosSynced synced:  Pos: %v", pos.Pos)
	h.binlogGTID = set.String()
	// log.Printf("OnPosSynced synced: %s, GTID: %v", set.String(), force)
	return nil
}
func (h *EventHandler) String() string {
	return "MySQLInputEventHandler"
}
