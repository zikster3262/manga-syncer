package runner

import (
	"context"
	"net/http"
	"time"

	graceful "gopkg.in/tylerb/graceful.v1"
)

type ServerRunner struct {
	srv     *graceful.Server
	timeout time.Duration
}

func NewServer(srv *http.Server, stopTimeout time.Duration) Runner {
	return ServerRunner{
		srv: &graceful.Server{
			Server:           srv,
			NoSignalHandling: true,
		},
		timeout: stopTimeout,
	}
}

func (s ServerRunner) Run(ctx context.Context) error {
	errch := make(chan error, 1)

	go func() { errch <- s.srv.ListenAndServe() }()

	select {
	case err := <-errch:
		return err
	case <-ctx.Done():
		s.srv.Stop(s.timeout)
		<-s.srv.StopChan()
		if err := <-errch; err != nil {
			return err
		}
		return ctx.Err()
	}
}
