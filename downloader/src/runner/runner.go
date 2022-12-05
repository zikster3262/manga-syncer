package runner

import (
	"context"
	"goquery-client/src/utils"

	"golang.org/x/sync/errgroup"
)

type Runner interface {
	Run(context.Context) error
}

func RunParallel(c context.Context, runners ...Runner) error {
	g, newCtx := errgroup.WithContext(c)
	for _, r := range runners {
		r := r
		g.Go(func() (err error) {
			if err = r.Run(newCtx); err != nil && err != context.Canceled {
				utils.FailOnError("runner", err)
			}
			return err
		})
	}
	return g.Wait()
}

type RunnerFunc func(context.Context) error

func (f RunnerFunc) Run(c context.Context) error {
	return f(c)
}
