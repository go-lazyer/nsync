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
	if p.cleaners == nil {
		p.cleaners = make(map[string]Cleaner)
	}
	p.cleaners[cleaner.Name()] = cleaner

}
func (p *Cleaning) Cleaning(event *input.Event) ([]input.Event, error) {
	if len(p.cleaners) == 0 {
		return []input.Event{*event}, nil
	}
	cleaner := p.cleaners[fmt.Sprintf("%s.%s", event.SourceDatabase, event.SourceTable)]
	if cleaner == nil {
		return []input.Event{*event}, nil
	}
	return cleaner.Cleaning(event)
}
