package rabbitmqtools

type Queue struct {
	Keys []string
	Name string
}

func (queue *Queue) AddKey(key string) {
	queue.Keys = append(queue.Keys, key)
}

func (queue *Queue) AddKeys(keys []string) {
	queue.Keys = append(queue.Keys, keys...)
}
