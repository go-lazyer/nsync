package processor

import (
	"testing"

	"github.com/go-lazyer/nsync/input"
)

type fakeInput struct {
	eventC chan *input.Event
}

func (f *fakeInput) GetEvent() <-chan *input.Event {
	return f.eventC
}

func (f *fakeInput) CountEvent() int {
	return len(f.eventC)
}

func (f *fakeInput) Close() {
	if f != nil && f.eventC != nil {
		close(f.eventC)
	}
}

func (f *fakeInput) SavePosition(string) error {
	return nil
}

func (f *fakeInput) SubmitEvent(event *input.Event) {
	f.eventC <- event
}

func (f *fakeInput) TotalEvent() int64 {
	return 0
}

func TestInputManagerCloseOnce(t *testing.T) {
	var m input.InputManager
	m.Close()
	m.Close()
	if got := m.GetEvent(); got != nil {
		t.Fatal("expected zero-value input manager to remain safe on repeated close")
	}
}
