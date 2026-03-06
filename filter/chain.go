package filter

import (
	"sync"

	"github.com/go-lazyer/nsync/config"
)

// Filter 处理接口
type Filter interface {
	Apply(database, table string) (string, string, bool) // 返回true表示继续处理，false表示丢弃
}

// Event 包含处理所需的数据结构

// 过滤器链实现
type Chain struct {
	filters []Filter
	lock    sync.RWMutex
}

func NewChain(filter *config.Filter) (*Chain, error) {

	chain := Chain{
		filters: make([]Filter, 0),
	}
	schemas, err := NewDatabases(filter.Databases)
	if err != nil {
		return &chain, err
	}
	chain.Add(schemas)

	tables, err := NewTables(filter)
	if err != nil {
		return &chain, err
	}
	chain.Add(tables)
	return &chain, nil
}
func (c *Chain) Add(filter Filter) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.filters = append(c.filters, filter)
}

func (c *Chain) Apply(database, table string) (string, string, bool) {
	var outputDatabase, outputTable string
	var ok bool
	for _, f := range c.filters {
		outputDatabase, outputTable, ok = f.Apply(database, table)
		if !ok {
			return outputDatabase, outputTable, ok
		}
	}
	return outputDatabase, outputTable, true
}
