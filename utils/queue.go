package utils

type Queue[T any] struct {
	items []T
}

func NewQueue[T any](myList []T) *Queue[T] {
	queue := Queue[T]{}
	queue.items = myList
	return &queue
}

func (q *Queue[T]) PushBack(item T) {
	q.items = append(q.items, item)
}

func (q *Queue[T]) PushBackAll(items ...T) {
	for _, val := range items {
		q.PushBack(val)
	}
}

func (q *Queue[T]) PushFront(item T) {
	q.items = append([]T{item}, q.items...)
}

func (q *Queue[T]) Peek() T {
	return q.items[0]
}

func (q *Queue[T]) Pop() T {
	item := q.Peek()
	q.items = q.items[1:]

	return item
}

func (q *Queue[T]) Clear() {
	q.items = []T{}
}

func (q *Queue[T]) Size() int {
	return len(q.items)
}

func (q *Queue[T]) IsEmpty() bool {
	return q.Size() == 0
}

func (q *Queue[T]) ToSlice() []T {
	dst := make([]T, len(q.items))
	copy(dst, q.items)
	return dst
}
