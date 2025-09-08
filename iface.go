package sseuda

type StorageEngine interface {
}

type Iterator interface {
	First() bool
	Seek(key []byte) bool

	Valid() bool
	Next() bool

	Key() []byte
	Value() []byte

	Close() error
}
