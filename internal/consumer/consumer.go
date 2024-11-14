package consumer

import (
	"log"
	"github.com/streadway/amqp"
)

func StartConsuming(process func([]byte)) error {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	q, err := ch.QueueDeclare("minha_fila", true, false, false, false, nil)
	if err != nil {
		return err
	}

	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for msg := range msgs {
			log.Printf("Mensagem recebida: %s", msg.Body)
			process(msg.Body)
		}
	}()

	select {}
}
