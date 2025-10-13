package filter

import (
	"sync"

	"github.com/go-lazyer/nsync/config"
	"github.com/go-lazyer/nsync/input"
)

// Filter 处理接口
type Filter interface {
	Apply(event *input.Event) bool // 返回true表示继续处理，false表示丢弃
}

// Event 包含处理所需的数据结构

// 过滤器链实现
type Chain struct {
	filters []Filter
	lock    sync.RWMutex
}

func NewChain(cfg *config.Config) (*Chain, error) {

	chain := Chain{
		filters: make([]Filter, 0),
	}
	schemas, err := NewDatabases(cfg.Filter.Databases)
	if err != nil {
		return &chain, err
	}
	chain.Add(schemas)

	tables, err := NewTables(cfg)
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

func (c *Chain) Apply(event *input.Event) bool {
	for _, f := range c.filters {
		if !f.Apply(event) {
			return false
		}
	}
	return true
}
