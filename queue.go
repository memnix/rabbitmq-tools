package rabbitmqtools

// Queue is a struct that contains the name and keys of a queue
type Queue struct {
	Keys []string // RabbitMQ queue keys
	Name string   // RabbitMQ queue name
}

// AddKey adds the given key to the queue
func (queue *Queue) AddKey(key string) {
	queue.Keys = append(queue.Keys, key)
}

// AddKeys adds the given keys to the queue
func (queue *Queue) AddKeys(keys []string) {
	queue.Keys = append(queue.Keys, keys...)
}
