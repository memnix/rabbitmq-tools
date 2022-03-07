package rabbitmqtools

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQConnection struct {
	conn     *amqp.Connection
	channel  *amqp.Channel
	queues   map[string]Queue
	exchange string
	error    chan error
}

func (connection *RabbitMQConnection) CloseConnection() error {
	err := connection.conn.Close()
	if err != nil {
		return fmt.Errorf("failed to close connection: %s", err)
	}
	return nil
}

func (connection *RabbitMQConnection) CloseChannel() error {
	err := connection.channel.Close()
	if err != nil {
		return fmt.Errorf("failed to close channel: %s", err)
	}
	return nil
}

func (connection *RabbitMQConnection) GetQueue(name string) Queue {
	return connection.queues[name]
}

func (connection *RabbitMQConnection) Set(conn *amqp.Connection, channel *amqp.Channel, queues map[string]Queue, exchange string) {
	connection.conn = conn
	connection.channel = channel
	connection.queues = queues
	connection.exchange = exchange
}

func (connection *RabbitMQConnection) AddQueue(queue Queue) error {
	connection.queues[queue.Name] = queue
	if _, err := connection.channel.QueueDeclare(queue.Name, true, false, false, false, nil); err != nil {
		return err
	}
	for _, k := range queue.Keys {
		if err := connection.channel.QueueBind(queue.Name, k, connection.exchange, false, nil); err != nil {
			return err
		}
	}

	return nil
}

func (connection *RabbitMQConnection) Consume() (map[string]<-chan amqp.Delivery, error) {
	m := make(map[string]<-chan amqp.Delivery)
	for _, q := range connection.queues {
		deliveries, err := connection.channel.Consume(q.Name, "", true, false, false, false, nil)
		if err != nil {
			return nil, err
		}
		m[q.Name] = deliveries
	}
	return m, nil
}

func (connection *RabbitMQConnection) RaiseError(err error) {
	connection.error <- err
}

func (connection *RabbitMQConnection) InitConnection(url string, exchange string) error {
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
