package utils

type PersistentLog[T any] interface {
	Append(element T) error
}
