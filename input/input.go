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
	PrimaryKey   []string // 主键
	Columns      []string // 列名
	Action       string   // insert/update/delete
	Rows         [][]any
	Position     string // 事件位置
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
