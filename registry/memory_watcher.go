package registry

import (
	"errors"
	"sync"
)

type memWatcher struct {
	wo   WatchOptions
	res  chan *Result
	exit chan bool
	id   string
	reg  *memRegistry
	once sync.Once
}

func (m *memWatcher) Next() (*Result, error) {
	for {
		select {
		case r := <-m.res:
			if len(m.wo.Service) > 0 && m.wo.Service != r.Service.Name {
				continue
			}
			return r, nil
		case <-m.exit:
			return nil, errors.New("watcher stopped")
		}
	}
}

func (m *memWatcher) Stop() {
	m.once.Do(func() {
		close(m.exit)
		if m.reg != nil {
			m.reg.removeWatcher(m.id)
		}
	})
}
