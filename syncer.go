package syncer

import (
	"fmt"
	"github.com/adminium/logger"
	"sync"
	"time"
)

type Conf struct {
	blockDuration time.Duration
	loggerModule  string
	dataChanSize  int
	retryDuration time.Duration
}

type Option func(conf *Conf)

func WithBlockDuration(duration time.Duration) Option {
	return func(conf *Conf) {
		if duration > 0 {
			conf.blockDuration = duration
		}
	}
}

func WithLoggerModule(name string) Option {
	return func(conf *Conf) {
		if name != "" {
			conf.loggerModule = name
		}
	}
}

func WithDataChanSize(size int) Option {
	return func(conf *Conf) {
		if size > 0 {
			conf.dataChanSize = size
		}
	}
}

func WithRetryDuration(duration time.Duration) Option {
	return func(conf *Conf) {
		if duration > 0 {
			conf.retryDuration = duration
		}
	}
}

func NewSyncer[T any](name string, options ...Option) *Syncer[T] {
	conf := &Conf{
		blockDuration: time.Minute,
		loggerModule:  fmt.Sprintf("syncer::%s", name),
		dataChanSize:  1024,
		retryDuration: 2 * time.Second,
	}

	for _, option := range options {
		option(conf)
	}

	return &Syncer[T]{
		name:      name,
		conf:      conf,
		log:       logger.NewLogger(conf.loggerModule),
		restartC:  make(chan struct{}),
		stopC:     make(chan struct{}),
		data:      make(chan T, conf.dataChanSize),
		stopped:   false,
		once:      sync.Once{},
		updatedAt: time.Now(),
	}
}

type Syncer[T any] struct {
	name      string
	log       *logger.Logger
	restartC  chan struct{}
	stopC     chan struct{}
	data      chan T
	stopped   bool
	once      sync.Once
	updatedAt time.Time
	conf      *Conf
}

func (s *Syncer[T]) Run(querier func(s *Syncer[T]) (clean func(), err error), consumer func(item T) error) {
	s.once.Do(func() {
		s.run(querier, consumer)
	})
}
func (m *Syncer[T]) Close() {
	defer func() {
		recover()
	}()
	m.stopped = true
	m.stopC <- struct{}{}
	close(m.restartC)
	close(m.stopC)
	close(m.data)
}

func (s *Syncer[T]) run(querier func(s *Syncer[T]) (clean func(), err error), consumer func(item T) error) {
	go s.consume(consumer)
Start:
	clean, err := querier(s)
	if err != nil {
		s.log.Errorf("exec syncer quering error: %s, restart after: %s", err, s.conf.retryDuration)
		time.Sleep(s.conf.retryDuration)
		goto Start
	}
	select {
	case <-s.restartC:
		s.log.Infof("restart syncer")
		if clean != nil {
			clean()
		}
		goto Start
	case <-s.stopC:
		s.log.Infof("stop syncer")
		if clean != nil {
			clean()
		}
		return
	}
}

func (s *Syncer[T]) consume(consumer func(item T) error) {
	ticker := time.NewTicker(s.conf.blockDuration)
	for {
		select {
		case <-ticker.C:
			if time.Now().Sub(s.updatedAt) > s.conf.blockDuration {
				s.log.Errorf("consume data timeout, restart after: %s", s.conf.retryDuration)
				time.Sleep(s.conf.retryDuration)
				s.updatedAt = time.Now()
				s.restart()
			}
		case item := <-s.data:
			s.updatedAt = time.Now()
			if err := consumer(item); err != nil {
				s.log.Errorf("consume item error: %s", err)
			}
		case <-s.stopC:
			s.log.Infof("exit consumer")
			ticker.Stop()
			return
		}
	}
}

func (s *Syncer[T]) restart() {
	s.restartC <- struct{}{}
}
