package input

import (
	"fmt"
	"reflect"

	"github.com/go-lazyer/nsync/config"
)

type Event struct {
	SourceSchema string // 源数据库的Schema
	SourceTable  string // 源数据库的Table
	Schema       string
	Table        string
	Columns      []string // 列名
	ColumnPkeys  []int    // 主键下标
	ColumnTypes  []string // 列名类型
	Action       string   // insert/update/delete
	BeforeRow    []any
	AfterRow     []any
	Position     string // 事件位置
}

func (e *Event) GetKeysByColumns(columns ...string) map[string]int {
	colIndex := make(map[string]int)
	for i, col := range e.Columns {
		colIndex[col] = i
	}
	return colIndex
}

type Input interface {
	GetEvent() <-chan *Event
	CloseEvent()
	SavePosition(position string) error // 保存当前处理位置
}

func NewInput(cfg *config.InputConfig) (Input, error) {
	if !reflect.ValueOf(cfg.Mysql).IsZero() {
		return NewMysqlInput(cfg)
	}
	return nil, fmt.Errorf("input config is empty")
}
