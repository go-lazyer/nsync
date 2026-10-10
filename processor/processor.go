package processor

import (
	"context"
	"fmt"
	"log"

	"github.com/go-lazyer/nsync/cleaning"
	"github.com/go-lazyer/nsync/config"
	"github.com/go-lazyer/nsync/filter"
	"github.com/go-lazyer/nsync/input"
	"github.com/go-lazyer/nsync/output"
)

type Processor struct {
	input    *input.InputManager
	output   output.Output
	cleaning *cleaning.Cleaning
	doneChan chan struct{}
}

func NewProcessor(cfg *config.Config, c *cleaning.Cleaning) (*Processor, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config is empty")
	}

	filterChain, err := filter.NewChain(&cfg.Filter)
	if err != nil {
		return nil, fmt.Errorf("failed to init filter: %w", err)
	}
	inputManager, err := input.NewInput(&cfg.Input, filterChain)
	if err != nil {
		return nil, err
	}
	proc := &Processor{
		input:    inputManager,
		doneChan: make(chan struct{}),
		cleaning: c,
	}

	out, err := output.NewOutput(&cfg.Output)
	if err != nil {
		return nil, err
	}
	proc.output = out

	return proc, nil
}

func (p *Processor) GetEvent() <-chan *input.Event {
	if p == nil {
		return nil
	}
	return p.input.GetEvent()
}

func (p *Processor) Run() error {
	defer close(p.doneChan)

	eventChan := p.GetEvent()
	if eventChan == nil {
		return nil
	}
	for event := range eventChan {
		if p.cleaning == nil {
			continue
		}
		// 这个应该注意 Cleaning 扩展出来的多个events 只有最后一个事件才会保存位置，前面生成的事件都不保存位置，因为他们都是基于同一个输入事件生成的
		events, err := p.cleaning.Cleaning(event)
		if err != nil {
			log.Printf("err:%v;event:%v\n", err, event)
			return err
		}
		if len(events) == 0 {
			continue
		}

		for _, e := range events {
			positionMap, err := p.output.Buffer(&e)
			if err != nil {
				return err
			}
			if len(positionMap) > 0 {
				p.input.SavePosition(positionMap)
			}
		}
	}
	return nil
}

func (p *Processor) SubmitEvent(event *input.Event) {
	if event == nil {
		return
	}
	p.input.SubmitEvent(event)
}
func (p *Processor) CountInputEventQueue() int {
	return p.input.CountEvent()
}
func (p *Processor) CountOutputEventBuffer() int {
	if p.output == nil {
		return 0
	}
	return p.output.CountBufferEvent()
}
func (p *Processor) TotalEvent() int64 {
	return p.input.TotalEvent()
}

// processor.go
func (p *Processor) Shutdown(ctx context.Context) error {
	p.input.Close()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.doneChan:
	}

	if p.output == nil {
		return nil
	}

	positionMap, err := p.output.Flush(true)
	if err != nil {
		log.Printf("Failed to Shutdown output buffer: %v", err)
		return err
	}
	if len(positionMap) > 0 {
		p.input.SavePosition(positionMap)
	}
	return nil
}
