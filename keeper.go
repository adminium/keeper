package keeper

import (
	"fmt"
	"github.com/adminium/async/bucket"
	"github.com/adminium/logger"
	"go.uber.org/atomic"
	"go.uber.org/multierr"
	"sync"
	"time"
)

var NewLogger = func(module string) logger.EventLogger {
	return logger.NewLogger(module)
}

type Action int

const (
	Quit    Action = 0
	Restart Action = 1
)

type Conf struct {
	errorAction     Action
	blockWaitTime   time.Duration
	loggerModule    string
	dataChanSize    int
	restartWaitTime time.Duration
	logger          logger.EventLogger
	bucket          bool
	bucketThreshold uint
	bucketInterval  time.Duration
}

type Option func(conf *Conf)

func WithErrorAction(action Action) Option {
	return func(conf *Conf) {
		conf.errorAction = action
	}
}

func WithBlockWaitTime(duration time.Duration) Option {
	return func(conf *Conf) {
		if duration > 0 {
			conf.blockWaitTime = duration
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

func WithRestartWaitTime(duration time.Duration) Option {
	return func(conf *Conf) {
		if duration > 0 {
			conf.restartWaitTime = duration
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

func WithBucket(threshold uint, interval time.Duration) Option {
	return func(conf *Conf) {
		if threshold > 0 || interval > 0 {
			conf.bucket = true
		}
		if threshold > 0 {
			conf.bucketThreshold = threshold
		}
		if interval > 0 {
			conf.bucketInterval = interval
		}
		return
	}
}

type Producer[T any] func(k *Keeper[T]) (clean func(), err error)
type Consumer[T any] func(k *Keeper[T], items []T) (err error)

func NewKeeper[T any](name string, options ...Option) *Keeper[T] {
	conf := &Conf{
		blockWaitTime:   time.Minute,
		loggerModule:    fmt.Sprintf("keeper::%s", name),
		dataChanSize:    1024,
		restartWaitTime: 2 * time.Second,
		bucket:          false,
		bucketThreshold: 100,
		bucketInterval:  5 * time.Second,
	}
	conf.logger = NewLogger(conf.loggerModule)
	for _, option := range options {
		option(conf)
	}
	return &Keeper[T]{
		index:     atomic.NewInt64(0),
		name:      name,
		conf:      conf,
		log:       conf.logger,
		restartC:  make(chan error, 1),
		stopC:     make(chan struct{}, 1),
		data:      make(chan []T, conf.dataChanSize),
		updatedAt: time.Now(),
		cleaner: &cleaner{
			log:    conf.logger,
			Mutex:  sync.Mutex{},
			cleans: nil,
		},
	}
}

type Keeper[T any] struct {
	name       string
	store      sync.Map
	log        Logger
	restartC   chan error
	stopC      chan struct{}
	data       chan []T
	updatedAt  time.Time
	conf       *Conf
	producer   Producer[T]
	consumer   Consumer[T]
	cleaner    *cleaner
	index      *atomic.Int64
	bucket     *bucket.Bucket[T]
	stopped    atomic.Bool
	closed     atomic.Bool
	running    atomic.Bool
	restarting atomic.Bool
}

func (k *Keeper[T]) Set(key string, value any) {
	k.store.Store(key, value)
}

func (k *Keeper[T]) Get(key string) any {
	v, _ := k.store.Load(key)
	return v
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

func (k *Keeper[T]) Run() (err error) {
	if k.running.CompareAndSwap(false, true) {
		err = k.run()
		if err != nil {
			return
		}
		return
	} else {
		err = fmt.Errorf("instance is running")
		return
	}
}

func (k *Keeper[T]) Produce(item T) {
	if k.stopped.Load() {
		return
	}
	if k.bucket != nil {
		err := k.bucket.Push(item)
		if err != nil {
			k.log.Errorf("push item to bucket err: %s", err)
		}
	} else {
		k.data <- []T{item}
	}
}

func (k *Keeper[T]) Index() int64 {
	return k.index.Load()
}

func (k *Keeper[T]) UpdatedAt() time.Time {
	return k.updatedAt
}

func (k *Keeper[T]) Stop() {
	if !k.stopped.CompareAndSwap(false, true) {
		return
	}
	k.stopC <- struct{}{}
}

func (k *Keeper[T]) close() {
	if !k.closed.CompareAndSwap(false, true) {
		return
	}
	close(k.restartC)
	close(k.stopC)
	close(k.data)
	if k.bucket != nil {
		k.bucket.Stop()
	}
}

func (k *Keeper[T]) run() (err error) {

	defer func() {
		k.Stop()
		k.close()
	}()

	go k.consume()
	if k.producer == nil {
		k.Log().Warnf("producer is nil")
		return
	}

	if k.conf.bucket {
		k.bucket = bucket.NewBucket[T](k.conf.bucketThreshold, k.conf.bucketInterval, func(data []T) {
			k.data <- data
		})
		k.bucket.SetLog(k.log)
		go k.bucket.Start()
	}
Start:
	err = nil
	k.index.Add(1)

	go func() {
		clean, e := k.producer(k)
		k.cleaner.addClean(clean)
		if e != nil {
			k.Restart(fmt.Errorf("exec producer error: %s", e))
			return
		}
		k.restarting.Store(false)
		k.log.Infof("start")
	}()

	select {
	case err = <-k.restartC:
		switch k.conf.errorAction {
		case Quit:
			if err != nil {
				k.log.Infof("quit with error: %v", err)
			}
			return
		case Restart:
			k.updatedAt = time.Now()
			goto Start
		default:
			err = fmt.Errorf("unsupported error action: %d", k.conf.errorAction)
			return
		}
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
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if time.Now().Sub(k.updatedAt) > k.conf.blockWaitTime {
				k.Restart(fmt.Errorf("consume data timeout"))
			}
		case items := <-k.data:
			k.updatedAt = time.Now()
			if err := k.consumer(k, items); err != nil {
				k.Restart(fmt.Errorf("consume data error: %s", err))
			}
		case <-k.stopC:
			return
		}
	}
}

func (k *Keeper[T]) Restart(err ...error) {
	e := multierr.Combine(err...)
	if !k.restarting.CompareAndSwap(false, true) {
		return
	}
	if k.stopped.Load() {
		return
	}
	k.log.Infof("handel restart signal: %v, restart after: %s", e, k.conf.restartWaitTime)
	k.cleaner.clean()
	time.Sleep(k.conf.restartWaitTime)
	k.restartC <- e
}
