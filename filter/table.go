package filter

import (
	"fmt"
	"strings"

	"github.com/go-lazyer/nsync/config"
)

type Tables struct {
	tables map[string]config.Table // key: input schema.table, value: output schema.table
}

func NewTables(fliter *config.Filter) (*Tables, error) {
	tables := make(map[string]config.Table, 0)

	for _, table := range fliter.Tables {
		if !strings.Contains(table.Input, ".") {
			return nil, fmt.Errorf("table input %s is not valid, it should be in the format of schema.table", table.Input)
		}
		if !strings.Contains(table.Output, ".") {
			return nil, fmt.Errorf("table output %s is not valid, it should be in the format of schema.table", table.Output)
		}
		tables[table.Input] = table
	}

	return &Tables{tables}, nil
}

func (f *Tables) Apply(database, table string) (string, string, bool) {
	if len(f.tables) == 0 {
		return database, table, false
	}
	key := database + "." + table
	outputTable, ok := f.tables[key]
	if !ok {
		return database, table, false
	}
	value := strings.Split(outputTable.Output, ".")
	return value[0], value[1], true
}
