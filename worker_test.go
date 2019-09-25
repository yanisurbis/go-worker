package worker

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunJobs(t *testing.T) {
	t.Run("run each function only once", func(t *testing.T) {
		var recieved uint32
		jobs := []job{
			job(func() error {
				time.Sleep(time.Second * 1)
				atomic.AddUint32(&recieved, 11)
				return nil
			}),
			job(func() error {
				time.Sleep(time.Second * 1)
				atomic.AddUint32(&recieved, 13)
				return nil
			}),
			job(func() error {
				time.Sleep(time.Second * 1)
				atomic.AddUint32(&recieved, 17)
				return nil
			}),
		}
		RunJobs(jobs, 3, 0)
		assert.Equal(t, 41, int(recieved))
	})

	t.Run("jobs are not started after specified amount of errors", func(t *testing.T) {
		var recieved uint32
		jobs := []job{
			job(func() error {
				time.Sleep(time.Second * 1)
				atomic.AddUint32(&recieved, 1)
				return errors.New("oops")
			}),
			job(func() error {
				time.Sleep(time.Second * 1)
				atomic.AddUint32(&recieved, 1)
				return errors.New("oops")
			}),
			job(func() error {
				time.Sleep(time.Second * 1)
				atomic.AddUint32(&recieved, 1)
				return errors.New("oops")
			}),
			job(func() error {
				time.Sleep(time.Second * 1)
				atomic.AddUint32(&recieved, 1)
				return errors.New("oops")
			}),
			job(func() error {
				time.Sleep(time.Second * 1)
				atomic.AddUint32(&recieved, 1)
				return errors.New("oops")
			}),
		}
		RunJobs(jobs, 1, 2)
		assert.Equal(t, 3, int(recieved))
	})

	t.Run("running limited number of jobs", func(t *testing.T) {
		var recieved uint32
		jobs := []job{
			job(func() error {
				time.Sleep(time.Second * 1)
				atomic.AddUint32(&recieved, 1)
				return errors.New("oops")
			}),
			job(func() error {
				time.Sleep(time.Second * 1)
				atomic.AddUint32(&recieved, 1)
				return errors.New("oops")
			}),
			job(func() error {
				time.Sleep(time.Second * 1)
				atomic.AddUint32(&recieved, 1)
				return errors.New("oops")
			}),
			job(func() error {
				time.Sleep(time.Second * 1)
				atomic.AddUint32(&recieved, 1)
				return errors.New("oops")
			}),
			job(func() error {
				time.Sleep(time.Second * 1)
				atomic.AddUint32(&recieved, 1)
				return errors.New("oops")
			}),
		}
		RunJobs(jobs, 2, 2)
		assert.Equal(t, 4, int(recieved))
	})
}
