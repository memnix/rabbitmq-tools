package rabbitmqtools

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitMQConnection is a struct that contains the connection and channel to RabbitMQ
type RabbitMQConnection struct {
	conn     *amqp.Connection // RabbitMQ connection
	channel  *amqp.Channel    // RabbitMQ channel
	queues   map[string]Queue // RabbitMQ queues
	exchange string           // RabbitMQ exchange
	error    chan error       // RabbitMQ error channel
}

// CloseConnection closes the connection to RabbitMQ
func (connection *RabbitMQConnection) CloseConnection() error {
	// will close() the connection
	err := connection.conn.Close()
	if err != nil {
		return fmt.Errorf("failed to close connection: %s", err)
	}
	return nil
}

// CloseChannel closes the channel to RabbitMQ
func (connection *RabbitMQConnection) CloseChannel() error {
	// will close() the channel
	err := connection.channel.Close()
	if err != nil {
		return fmt.Errorf("failed to close channel: %s", err)
	}
	return nil
}

// GetQueue returns the queue with the given name from the connection
//
//	@Description: returns the queue with the given name from the connection
//	@receiver connection - RabbitMQConnection
//	@param name - string - name of the queue
//	@return Queue - the queue with the given name
//	@return error - error if the queue does not exist
func (connection *RabbitMQConnection) GetQueue(name string) (Queue, error) {
	if queue, ok := connection.queues[name]; ok {
		return queue, nil
	}
	return Queue{}, fmt.Errorf("queue %s not found", name)
}

// Set sets the connection and channel to RabbitMQ
func (connection *RabbitMQConnection) Set(conn *amqp.Connection, channel *amqp.Channel, queues map[string]Queue, exchange string) {
	connection.conn = conn         // RabbitMQ connection
	connection.channel = channel   // RabbitMQ channel
	connection.queues = queues     // RabbitMQ queues
	connection.exchange = exchange // RabbitMQ exchange
}

// AddQueues adds the given queues to the connection
func (connection *RabbitMQConnection) AddQueues(queues []Queue) error {
	for _, q := range queues {
		connection.queues[q.Name] = q
		if _, err := connection.channel.QueueDeclare(q.Name, true, false, false, false, nil); err != nil {
			return err
		}
		for _, k := range q.Keys {
			if err := connection.channel.QueueBind(q.Name, k, connection.exchange, false, nil); err != nil {
				return err
			}
		}
	}
	return nil
}

// Consume consumes the queues
func (connection *RabbitMQConnection) Consume() (map[string]<-chan amqp.Delivery, error) {
	m := make(map[string]<-chan amqp.Delivery) // map of channels
	// consume all queues
	for _, q := range connection.queues {
		// consume the queue
		deliveries, err := connection.channel.Consume(q.Name, "", true, false, false, false, nil)
		if err != nil {
			return nil, err
		}
		m[q.Name] = deliveries
	}
	return m, nil
}

// RaiseError raises an error on the error channel
func (connection *RabbitMQConnection) RaiseError(err error) {
	connection.error <- err
}

// InitConnection initializes the connection to RabbitMQ
func (connection *RabbitMQConnection) InitConnection(url, exchange string) error {
	conn, err := amqp.Dial(url)
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %s", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open a chanel: %s", err)
	}

	err = ch.ExchangeDeclare(
		exchange, // name
		"topic",  // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare an exchange: %s", err)
	}

	queues := make(map[string]Queue)

	connection.Set(conn, ch, queues, exchange)

	return nil
}
