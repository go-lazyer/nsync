package processor

import (
	"context"
	"log"

	"github.com/go-lazyer/nsync/cleaning"
	"github.com/go-lazyer/nsync/input"
	"github.com/go-lazyer/nsync/output"
)

type Processor struct {
	input    input.Input
	output   output.Output
	cleaning *cleaning.Cleaning
	doneChan chan struct{}
}

func NewProcessor(input input.Input, cleaning *cleaning.Cleaning, output output.Output) *Processor {
	return &Processor{
		input:    input,
		cleaning: cleaning,
		output:   output,
		doneChan: make(chan struct{}), // 初始化通道
	}
}

func (p *Processor) Run() error {
	defer close(p.doneChan) // 确保结束时关闭通道
	eventChan := p.input.GetEvent()

	for event := range eventChan {
		// 这个应该注意 Cleaning 扩展出来的多个events 只有最后一个事件才会保存位置，前面生成的事件都不保存位置，因为他们都是基于同一个输入事件生成的
		events, err := p.cleaning.Cleaning(event)
		if err != nil {
			log.Printf("err:%v;event:%v\n", err, event)
			continue
		}
		if len(events) == 0 {
			continue
		}

		for _, e := range events {
			e.Database = "" // 清空数据库字段，避免输出到 manticore 时被当做集群名称
			savePosition, err := p.output.Buffer(&e)
			if err != nil {
				log.Printf("Failed to buffer event: %v", err)
				return err
			}
			if savePosition != "" {
				p.input.SavePosition(savePosition)
			}
		}
	}
	return nil
}

func (p *Processor) SubmitEvent(event *input.Event) {
	// 停止接收新事件
	p.input.SubmitEvent(event)
}
func (p *Processor) CountInputEventQueue() int {
	// 获取当前事件通道中的事件数量
	return p.input.CountEvent()
}
func (p *Processor) CountOutputEventBuffer() int {
	// 获取当前输出缓冲中的事件数量
	return p.output.CountBufferEvent()
}
func (p *Processor) TotalEvent() (int64, int64) {
	return p.input.TotalEvent()
}
func (p *Processor) FlushAll() error {
	// 获取当前输出缓冲中的事件数量
	// 刷新所有输出缓冲
	savePosition, err := p.output.FlushAll()
	if err != nil {
		log.Printf("Failed to FlushAll output buffer: %v", err)
		return err
	}
	if savePosition != "" {
		p.input.SavePosition(savePosition)
	}
	return nil
}

// processor.go
func (p *Processor) Shutdown(ctx context.Context) error {
	// 停止接收新事件
	p.input.CloseEvent()

	// 等待处理中的事件完成
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.doneChan: // 添加doneChan来通知处理完成
	}

	// 刷新所有输出缓冲
	savePosition, err := p.output.FlushAll()
	if err != nil {
		log.Printf("Failed to Shutdown output buffer: %v", err)
		return err
	}
	if savePosition != "" {
		p.input.SavePosition(savePosition)
	}
	return nil
}
