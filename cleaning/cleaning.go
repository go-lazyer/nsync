package cleaning

import (
	"github.com/go-lazyer/nsync/input"
)

type Cleaner interface {
	Cleaning(event *input.Event) bool // 返回true表示继续处理，false表示丢弃
}

// 数据清洗
type Cleaning struct {
	cleaners map[string]Cleaner
}

func NewCleaning() *Cleaning {
	return &Cleaning{}
}

func (p *Cleaning) Cleaning(event *input.Event) error {
	if len(p.cleaners) == 0 {
		return nil
	}
	cleaner := p.cleaners[event.SourceTable]
	if cleaner == nil {
		return nil
	}
	cleaner.Cleaning(event)
	return nil
}
