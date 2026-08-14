package cleaning

import (
	"fmt"

	"github.com/go-lazyer/nsync/input"
)

type Cleaner interface {
	Name() string
	Cleaning(event *input.Event) ([]input.Event, error)
}

// 数据清洗
type Cleaning struct {
	cleaners map[string]Cleaner
}

func NewCleaning() *Cleaning {
	return &Cleaning{}
}

func (p *Cleaning) AddCleaner(cleaner Cleaner) {
	if cleaner == nil {
		return
	}
	if p.cleaners == nil {
		p.cleaners = make(map[string]Cleaner)
	}
	p.cleaners[cleaner.Name()] = cleaner

}
func (p *Cleaning) Cleaning(event *input.Event) ([]input.Event, error) {
	// 如果没有注册任何 cleaner，直接返回原事件
	if len(p.cleaners) == 0 {
		return []input.Event{*event}, nil
	}
	// 根据事件的 source_database 和 source_table 查找对应的 cleaner
	cleaner := p.cleaners[fmt.Sprintf("%s.%s", event.SourceDatabase, event.SourceTable)]
	// 如果没有找到对应的 cleaner，直接返回原事件
	if cleaner == nil {
		return []input.Event{*event}, nil
	}
	events, err := cleaner.Cleaning(event)
	if err != nil {
		return nil, err
	}
	//如果events >1个，只保留最后一个的位置，前面生成的事件都不保存位置，因为他们都是基于同一个输入事件生成的
	for i := 0; i < len(events)-1; i++ {
		events[i].Position = ""
	}
	return events, err
}
