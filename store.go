package keeper

import "sync"

func newStore() *store {
	return &store{
		Mutex: sync.Mutex{},
		m:     make(map[string]any),
	}
}

type store struct {
	sync.Mutex
	m map[string]any
}

func (s *store) Set(key string, value any) {
	s.Lock()
	defer s.Unlock()
	s.m[key] = value
}

func (s *store) Get(key string) any {
	s.Lock()
	defer s.Unlock()
	return s.m[key]
}

func (s *store) Delete(key string) {
	s.Lock()
	defer s.Unlock()
	delete(s.m, key)
}
