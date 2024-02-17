package utils

type Set[T comparable] interface {
	Add(T)
	AddAll(...T)
	Contains(T) bool
	Remove(T)
	RemoveAll(...T)
	Clear()
	ToSlice() []T
	ForEach(func(T) bool)
	GetSize() int
}

type MapSet[T comparable] struct {
	internalMap map[T]struct{}
}

func NewMapSet[T comparable]() *MapSet[T] {
	return &MapSet[T]{internalMap: make(map[T]struct{})}
}

func NewMapSetFromElems[T comparable](elems ...T) *MapSet[T] {
	mapSet := NewMapSet[T]()
	mapSet.AddAll(elems...)
	return mapSet
}

func (m *MapSet[T]) Add(elem T) {
	m.internalMap[elem] = struct{}{}
}

func (m *MapSet[T]) AddAll(elems ...T) {
	for _, elem := range elems {
		m.Add(elem)
	}
}

func (m *MapSet[T]) Contains(elem T) bool {
	_, ok := m.internalMap[elem]
	return ok
}

func (m *MapSet[T]) Remove(elem T) {
	delete(m.internalMap, elem)
}

func (m *MapSet[T]) RemoveAll(elems ...T) {
	for _, elem := range elems {
		m.Remove(elem)
	}
}

func (m *MapSet[T]) Clear() {
	m.internalMap = map[T]struct{}{}
}

func (m *MapSet[T]) ToSlice() []T {
	var elems []T

	for elem := range m.internalMap {
		elems = append(elems, elem)
	}

	return elems
}

func (m *MapSet[T]) ForEach(myFunc func(T) bool) {
	for item := range m.internalMap {
		stop := myFunc(item)
		if stop {
			break
		}
	}
}

func (m *MapSet[T]) GetSize() int {
	return len(m.internalMap)
}
