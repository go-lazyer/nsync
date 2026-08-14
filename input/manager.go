package input

import (
	"fmt"
	"sync"

	"github.com/go-lazyer/nsync/config"
	"github.com/go-lazyer/nsync/filter"
)

type InputManager struct {
	inputs map[string]Input

	event chan *Event
	once  sync.Once
}

func NewInput(cfg *config.InputConfig, filter *filter.Chain) (*InputManager, error) {
	if cfg == nil {
		return nil, fmt.Errorf("input config is empty")
	}
	event := make(chan *Event, 10240)

	inputs := make(map[string]Input)
	for i := range cfg.Mysqls {
		src := &cfg.Mysqls[i]
		if src.Name == "" {
			return nil, fmt.Errorf("input.mysql[%d].name is empty", i)
		}
		in, err := NewMysqlInput(src, filter, event)
		if err != nil {
			return nil, err
		}

		inputs[src.Name] = in
	}
	for i := range cfg.Pgsqls {
		src := &cfg.Pgsqls[i]
		if src.Name == "" {
			return nil, fmt.Errorf("input.postgres[%d].name is empty", i)
		}
		in, err := NewPostgresInput(src, filter, event)
		if err != nil {
			return nil, err
		}
		inputs[src.Name] = in
	}
	if len(inputs) == 0 {
		return nil, fmt.Errorf("input config is empty")
	}
	return &InputManager{inputs: inputs, event: event}, nil
}

func (m *InputManager) GetEvent() <-chan *Event {
	if m == nil || m.event == nil {
		return nil
	}
	return m.event
}

func (m *InputManager) CountEvent() int {
	if m == nil || m.event == nil {
		return 0
	}
	return len(m.event)
}

func (m *InputManager) Close() {
	if m == nil {
		return
	}
	m.once.Do(func() {
		for _, in := range m.inputs {
			in.Close()
		}
		if m.event != nil {
			close(m.event)
			m.event = nil
		}
	})
}

func (m *InputManager) SavePosition(positionMap map[string]string) error {
	for sourceName, position := range positionMap {
		err := m.inputs[sourceName].SavePosition(position)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *InputManager) SubmitEvent(event *Event) {
	if m == nil || m.event == nil {
		return
	}
	m.event <- event
}

func (m *InputManager) TotalEvent() int64 {
	var total int64
	for _, in := range m.inputs {
		a := in.TotalEvent()
		total += a
	}
	return total
}
