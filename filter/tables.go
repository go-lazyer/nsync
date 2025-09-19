package filter

import (
	"fmt"
	"strings"

	"github.com/go-lazyer/nsync/config"
	"github.com/go-lazyer/nsync/input"
)

type Tables struct {
	tables map[string]config.Table // key: input schema.table, value: output schema.table
}

func NewTables(conf []config.Table) (*Tables, error) {
	tables := make(map[string]config.Table, 0)

	for _, table := range conf {
		if !strings.Contains(table.Input, ".") {
			return nil, fmt.Errorf("table input %s is not valid, it should be in the format of schema.table", table.Input)
		}
		if len(table.PrimaryKey) == 0 {
			return nil, fmt.Errorf("table %s primary key is empty", table.Input)
		}
		if !strings.Contains(table.Output, ".") {
			return nil, fmt.Errorf("table output %s is not valid, it should be in the format of schema.table", table.Output)
		}
		tables[table.Input] = table
	}
	return &Tables{tables}, nil
}

func (f *Tables) Apply(event *input.Event) bool {
	if len(f.tables) == 0 {
		return true
	}
	key := event.SourceSchema + "." + event.SourceTable
	table, ok := f.tables[key]
	if !ok {
		return false
	}
	value := strings.Split(table.Output, ".")
	event.Schema = value[0]
	event.Table = value[1]
	event.PrimaryKey = table.PrimaryKey
	return true
}
