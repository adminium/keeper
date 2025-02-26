package keeper

import (
	"fmt"
	"github.com/adminium/logger"
	"go.uber.org/multierr"
	"sync"
	"time"
)

type Conf struct {
	blockDuration time.Duration
	loggerModule  string
	dataChanSize  int
	retryDuration time.Duration
	logger        Logger
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

func WithLogger(logger Logger) Option {
	return func(conf *Conf) {
		if logger != nil {
			conf.logger = logger
		}
	}
}

type Producer[T any] func(k *Keeper[T]) (clean func(), err error)
type Consumer[T any] func(k *Keeper[T], item T) (err error)

func NewKeeper[T any](name string, options ...Option) *Keeper[T] {
	conf := &Conf{
		blockDuration: time.Minute,
		loggerModule:  fmt.Sprintf("keeper::%s", name),
		dataChanSize:  1024,
		retryDuration: 2 * time.Second,
	}
	conf.logger = logger.NewLogger(conf.loggerModule)
	for _, option := range options {
		option(conf)
	}
	return &Keeper[T]{
		name:      name,
		conf:      conf,
		log:       conf.logger,
		restartC:  make(chan struct{}),
		stopC:     make(chan struct{}),
		data:      make(chan T, conf.dataChanSize),
		stopped:   false,
		once:      sync.Once{},
		updatedAt: time.Now(),
		store:     make(map[string]any),
	}
}

type Keeper[T any] struct {
	name      string
	store     map[string]any
	log       Logger
	restartC  chan struct{}
	stopC     chan struct{}
	data      chan T
	stopped   bool
	once      sync.Once
	updatedAt time.Time
	conf      *Conf
	producer  Producer[T]
	consumer  Consumer[T]
}

func (k *Keeper[T]) Set(key string, value any) {
	k.store[key] = value
}

func (k *Keeper[T]) Get(key string) any {
	return k.store[key]
}

func (k *Keeper[T]) SetProducer(producer Producer[T]) {
	k.producer = producer
}

func (k *Keeper[T]) SetConsumer(consumer Consumer[T]) {
	k.consumer = consumer
}

func (k *Keeper[T]) Log() Logger {
	return k.log
}

func (k *Keeper[T]) Run() {
	k.once.Do(func() {
		k.run()
	})
}

func (k *Keeper[T]) Produce(item T) {
	if !k.stopped {
		k.data <- item
	}
}

func (k *Keeper[T]) Stop() {
	defer func() {
		recover()
	}()
	k.stopped = true
	k.stopC <- struct{}{}
	close(k.restartC)
	close(k.stopC)
	close(k.data)
}

func (k *Keeper[T]) run() {
	go k.consume()
	if k.producer == nil {
		k.Log().Warnf("producer is nil")
		return
	}
	var clean func()
Start:
	k.log.Infof("start")
	if clean != nil {
		clean()
	}

	go func() {
		var err error
		clean, err = k.producer(k)
		if err != nil {
			k.Restart(fmt.Errorf("exec producer error: %s", err))
			return
		}
	}()

	select {
	case <-k.restartC:
		goto Start
	case <-k.stopC:
		k.log.Infof("stop")
		return
	}
}

func (k *Keeper[T]) consume() {
	if k.consumer == nil {
		k.Log().Warnf("consumer is nil")
		return
	}
	ticker := time.NewTicker(k.conf.blockDuration)

	for {
		select {
		case <-ticker.C:
			if time.Now().Sub(k.updatedAt) > k.conf.blockDuration {
				k.updatedAt = time.Now()
				k.Restart(fmt.Errorf("consume data timeout"))
			}
		case item := <-k.data:
			k.updatedAt = time.Now()
			if err := k.consumer(k, item); err != nil {
				k.Restart(fmt.Errorf("consume data item error: %s", err))
			}
		case <-k.stopC:
			k.log.Infof("stop consuming")
			ticker.Stop()
			return
		}
	}
}

func (k *Keeper[T]) Restart(err ...error) {
	if !k.stopped {
		e := multierr.Combine(err...)
		if e != nil {
			k.log.Errorf("%s, restart after: %s", e, k.conf.retryDuration)
		} else {
			k.log.Infof("prepare to restart after: %s", k.conf.retryDuration)
		}
		time.Sleep(k.conf.retryDuration)
		k.restartC <- struct{}{}
	} else {
		k.log.Errorf("restart error, instance is stopped")
	}
}
