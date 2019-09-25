package worker

import (
	"sync"
	"sync/atomic"
)

type job func() error

func RunJobs(jobs []job, quotaLimit int, errorLimit int) {
	var timesFailed uint32
	wg := &sync.WaitGroup{}
	quotaCh := make(chan struct{}, quotaLimit)

	for i, j := range jobs {
		wg.Add(1)
		go func(wg *sync.WaitGroup, job job, quotaCh chan struct{}, timesFailed *uint32, i int) {
			defer wg.Done()
			quotaCh <- struct{}{}
			if int(*timesFailed) > errorLimit {
				<-quotaCh
				return
			}
			failed := job()
			if failed != nil {
				atomic.AddUint32(timesFailed, 1)
			}
			<-quotaCh
		}(wg, j, quotaCh, &timesFailed, i)
	}

	wg.Wait()
}
