package keeper

import "sync"

type cleaner struct {
	log Logger
	sync.Mutex
	cleans []func()
}

func (c *cleaner) addClean(f func()) {
	if f == nil {
		return
	}
	c.Lock()
	defer c.Unlock()
	c.cleans = append(c.cleans, f)
}

func (c *cleaner) clean() {
	c.Lock()
	defer c.Unlock()
	n := len(c.cleans)
	for _, v := range c.cleans {
		c.exec(v)
	}
	c.cleans = []func(){}
	if n > 0 {
		c.log.Infof("clean done")
	}
}

func (c *cleaner) exec(f func()) {
	defer func() {
		e := recover()
		if e != nil {
			c.log.Errorf("clean error: %v", e)
		}
	}()
	f()
}
