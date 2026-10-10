package input

const POSITION_FILE_NAME = "_position.info"

type Event struct {
	InputName      string
	SourceDatabase string // 源数据库的Schema
	SourceTable    string // 源数据库的Table
	Database       string
	Table          string
	Columns        []string // 列名
	ColumnPkeys    []int    // 主键下标
	ColumnTypes    []string // 列名类型
	Action         string   // insert/update/delete
	BeforeRow      []any
	AfterRow       []any
	Position       string // 事件位置
}

func (e *Event) GetKeysByColumns(columns ...string) map[string]int {
	colIndex := make(map[string]int)
	for i, col := range e.Columns {
		colIndex[col] = i
	}
	return colIndex
}

func (e *Event) ToBeforeMap() map[string]any {
	beforeMap := make(map[string]any)
	for i, col := range e.Columns {
		beforeMap[col] = e.BeforeRow[i]
	}
	return beforeMap
}

func (e *Event) ToBeforePkeyMap() map[string]any {
	beforeMap := make(map[string]any)
	for _, i := range e.ColumnPkeys {
		beforeMap[e.Columns[i]] = e.BeforeRow[i]
	}
	return beforeMap
}

func (e *Event) ToAfterMap() map[string]any {
	afterMap := make(map[string]any)
	for i, col := range e.Columns {
		afterMap[col] = e.AfterRow[i]
	}
	return afterMap
}

type Input interface {
	SavePosition(position string) error // 保存当前处理位置
	TotalEvent() int64
	Close()
}
