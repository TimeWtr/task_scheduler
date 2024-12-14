package task_scheduler

import (
	"container/heap"
	_const "github.com/TimeWtr/task_scheduler/const"
	"sync"
	"time"
)

type Cache interface {
	Set(key string, value any)
	Get(key string) (any, bool)
	Del(key string)
}

func NewLocalCache(size int) Cache {
	return &LocalCache{
		mp: make(map[string]any, size),
		mu: &sync.RWMutex{},
	}
}

type LocalCache struct {
	mp map[string]any
	mu *sync.RWMutex
}

func (l *LocalCache) Set(key string, value any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.mp[key] = value
}

func (l *LocalCache) Get(key string) (any, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	v, ok := l.mp[key]
	return v, ok
}

func (l *LocalCache) Del(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.mp, key)
}

// Job 小顶堆优先级延时队列
// 下次执行时间最近的Job放到队列头部
type Job struct {
	// ID 在数据库中的ID信息
	ID int
	// Job的唯一标识
	JobID string
	// Job的名称
	JobName string
	// Cfg 通用的配置信息
	Cfg string
	// 优先级
	Priority _const.Priority
	// NextExecTime 下次等待调度的时间
	NextExecTime int64
}

type Hp []Job

func (h *Hp) Len() int {
	return len(*h)
}

// Less 比较
// 条件：
// 1. 时间较小者为先执行的Job
// 2. 时间相同的情况下比较优先级，优先级更高的先执行，
func (h *Hp) Less(i, j int) bool {
	a, b := (*h)[i], (*h)[j]
	return a.NextExecTime < b.NextExecTime ||
		a.NextExecTime == b.NextExecTime && a.Priority.Less(b.Priority)
}

func (h *Hp) Swap(i, j int) {
	(*h)[i], (*h)[j] = (*h)[j], (*h)[i]
}

func (h *Hp) Push(x interface{}) {
	*h = append(*h, x.(Job))
}

func (h *Hp) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// CheckExistExecJob 判断队列中是否存在可执行的定时任务
func (h *Hp) CheckExistExecJob() bool {
	if len(*h) == 0 {
		return false
	}

	job := (*h)[h.Len()-1]
	return job.NextExecTime <= time.Now().Unix()
}

// ConcurrentJobHeap 并发优先级延时队列
type ConcurrentJobHeap struct {
	Hp
	mu *sync.RWMutex
}

func NewConcurrentJobHeap(size int) *ConcurrentJobHeap {
	return &ConcurrentJobHeap{
		Hp: make(Hp, 0, size),
		mu: &sync.RWMutex{},
	}
}

func (h *ConcurrentJobHeap) Push(value any) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.Hp.Push(value)
}

func (h *ConcurrentJobHeap) Pop() any {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.Len() == 0 {
		return nil
	}

	return heap.Pop(&h.Hp)
}

func (h *ConcurrentJobHeap) Empty() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.Len() == 0
}

func (h *ConcurrentJobHeap) CheckExistExecJob() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.Hp.CheckExistExecJob()
}
