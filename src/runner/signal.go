package runner

import (
	"context"
	"errors"
	"os"
	"os/signal"
)

var SignalReceived = errors.New("signal received")

type SigRunner struct {
	signals []os.Signal
}

func NewSignal(sig ...os.Signal) Runner {
	return SigRunner{sig}
}

func (s SigRunner) Run(ctx context.Context) error {
	c := make(chan os.Signal, 1)
	signal.Notify(c, s.signals...)
	defer signal.Stop(c)
	select {
	case <-c:
		return SignalReceived
	case <-ctx.Done():
		return ctx.Err()
	}

}
