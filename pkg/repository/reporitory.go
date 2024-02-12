package repository

import (
	"errors"
	"sync"
)

var (
	ErrNotFound = errors.New("not found")
)

type Repository struct {
	mutex sync.Mutex
	data  map[string][]byte
}

func NewRepository() *Repository {
	return &Repository{
		data: make(map[string][]byte),
	}
}

func (r *Repository) Get(key string) ([]byte, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	value, ok := r.data[key]
	if !ok {
		return nil, ErrNotFound
	}
	return value, nil
}

func (r *Repository) Set(key string, value []byte) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	r.data[key] = value
}

func (r *Repository) Dump() map[string][]byte {
	return r.data
}

func (r *Repository) Restore(data map[string][]byte) {
	r.data = data
}
