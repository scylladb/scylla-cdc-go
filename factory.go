package scyllacdc

import (
	"context"
	"time"
)

type FactoryConfig struct {
	Logger          Logger
	CommitInterval  time.Duration
	Callback        func(context.Context, Change) error
	ShutdownTimeout time.Duration
}

func NewFactory(config *FactoryConfig) *factory {
	return &factory{
		logger:          config.Logger,
		commitInterval:  config.CommitInterval,
		callback:        config.Callback,
		shutdownTimeout: config.ShutdownTimeout,
	}
}

type factory struct {
	logger          Logger
	commitInterval  time.Duration
	callback        func(context.Context, Change) error
	shutdownTimeout time.Duration
}

type consumer struct {
	reporter        *PeriodicProgressReporter
	callback        func(context.Context, Change) error
	shutdownTimeout time.Duration
}

func (f *factory) CreateChangeConsumer(ctx context.Context, input CreateChangeConsumerInput) (ChangeConsumer, error) {
	reporter := NewPeriodicProgressReporter(f.logger, f.commitInterval, input.ProgressReporter)
	reporter.Start(ctx)
	return &consumer{
		reporter:        reporter,
		callback:        f.callback,
		shutdownTimeout: f.shutdownTimeout,
	}, nil
}

func (c *consumer) Consume(ctx context.Context, change Change) error {
	err := c.callback(ctx, change)
	if err != nil {
		return err
	}
	c.reporter.Update(change.Time)
	return nil
}

func (c *consumer) End() error {
	ctx, cancel := context.WithTimeout(context.TODO(), c.shutdownTimeout)
	defer cancel()
	err := c.reporter.SaveAndStop(ctx)
	if err != nil {
		return err
	}
	return nil
}
