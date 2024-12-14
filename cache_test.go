package task_scheduler

import (
	"container/heap"
	"fmt"
	_const "github.com/TimeWtr/task_scheduler/const"
	"testing"
	"time"
)

func TestNewConcurrentJobHeap(t *testing.T) {
	h := NewConcurrentJobHeap(12)
	heap.Init(h)
	now := time.Now()
	heap.Push(h, Job{
		ID:           123243,
		JobName:      "t1",
		Priority:     _const.PriorityHigh,
		NextExecTime: now.Add(time.Second * 2).Unix(),
	})

	heap.Push(h, Job{
		ID:           12325,
		JobName:      "t2",
		Priority:     _const.PriorityMedium,
		NextExecTime: now.Add(time.Second * 2).Unix(),
	})

	heap.Push(h, Job{
		ID:           12334,
		JobName:      "t3",
		Priority:     _const.PriorityLow,
		NextExecTime: now.Add(time.Second * 13).Unix(),
	})

	heap.Push(h, Job{
		ID:           1234,
		JobName:      "t4",
		Priority:     _const.PriorityHigh,
		NextExecTime: now.Add(time.Second * 8).Unix(),
	})

	for {
		if h.Empty() {
			break
		}

		if !h.CheckExistExecJob() {
			continue
		}

		v := h.Pop()
		job, ok := v.(Job)
		if !ok {
			t.Fatalf("job not a job")
		}
		fmt.Println("time: ", time.Now().Unix())
		t.Logf("Job message: %+v\n", job)
	}
}
