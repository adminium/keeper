package keeper

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestKeeper(t *testing.T) {

	stop := make(chan struct{})
	defer close(stop)

	k := NewKeeper[int]("test",
		WithErrorAction(Restart),
		WithBlockWaitTime(5*time.Second),
		WithRestartWaitTime(2*time.Second),
	)
	i := 0
	k.SetProducer(func(k *Keeper[int]) (clean func(), err error) {
		ticker := time.NewTicker(1 * time.Second)
		if i == 0 {
			err = fmt.Errorf("init error")
			i++
			return
		}
		go func() {
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
					k.Produce(i)
					if i == 4 {
						k.Restart(fmt.Errorf("restart forcedly"))
					}
					if i == 6 {
						time.Sleep(8 * time.Second)
					}
					i++
				}
			}
		}()

		clean = func() {
			k.Log().Infof("close ticker")
			ticker.Stop()
		}

		return
	})

	k.SetConsumer(func(k *Keeper[int], items []int) (err error) {
		for _, item := range items {
			k.Log().Infof("receive item: %d", item)
			if item == 3 {
				stop <- struct{}{}
				return fmt.Errorf("manual error")
			}
			if item == 10 {
				k.Stop()
				return
			}
			return
		}
		return
	})

	_ = k.Run()

	return
}

func TestKeeperQuit(t *testing.T) {
	stop := make(chan struct{})
	defer close(stop)

	k := NewKeeper[int]("test")
	i := 0
	k.SetProducer(func(k *Keeper[int]) (clean func(), err error) {
		ticker := time.NewTicker(1 * time.Second)
		go func() {
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
					k.Produce(i)
					if i == 4 {
						k.Restart(fmt.Errorf("restart forcedly"))
					}
					i++
				}
			}
		}()
		clean = func() {
			k.Log().Infof("close ticker")
			ticker.Stop()
		}
		return
	})

	k.SetConsumer(func(k *Keeper[int], items []int) (err error) {
		for _, item := range items {
			k.Log().Infof("receive item: %d", item)
			return
		}
		return
	})

	err := k.Run()
	require.Error(t, err)

	return
}

func TestKeeperOnce(t *testing.T) {
	k := NewKeeper[int]("test", WithBlockWaitTime(3*time.Second))
	i := 0
	k.SetProducer(func(k *Keeper[int]) (clean func(), err error) {
		timer := time.NewTimer(1 * time.Second)
		select {
		case <-timer.C:
			k.Produce(i)
		}
		clean = func() {
			k.Log().Infof("close timer")
			timer.Stop()
		}
		return
	})

	k.SetConsumer(func(k *Keeper[int], items []int) (err error) {
		for _, item := range items {
			k.Log().Infof("receive item: %d", item)
			return
		}
		return
	})

	err := k.Run()
	require.Error(t, err)

	return
}
