package keeper

import (
	"fmt"
	"testing"
	"time"
)

func TestKeeper(t *testing.T) {

	stop := make(chan struct{})
	defer close(stop)

	k := NewKeeper[int]("test")
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
					i++
					if i == 5 {
						k.Restart(fmt.Errorf("restart forcedly"))
						//k.Stop()
						return
					}
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

	k.Run()

	return
}
