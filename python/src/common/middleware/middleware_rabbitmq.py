import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddlewareMessageError, MessageMiddlewareDisconnectedError, MessageMiddlewareCloseError

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host))
            channel = connection.channel()
            channel.queue_declare(queue=queue_name, durable=True)
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError(f"Not able to connect to {host}")
        except pika.exceptions.AMQPError:
            raise MessageMiddlewareMessageError(f"Not able to declare queue: '{queue_name}'")

        self.connection = connection
        self.channel = channel
        self.queue_name = queue_name
        self.consumer_tag = None

    def start_consuming(self, on_message_callback):
        def callback(ch, method, _properties, body):

            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)

            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag)

            return on_message_callback(body, ack, nack)

        try:
            self.channel.basic_qos(prefetch_count=1)
            self.consumer_tag = self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback)
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError(f"Connection lost while consuming from {self.queue_name}")
        except pika.exceptions.AMQPError:
            raise MessageMiddlewareMessageError(f"Error while consuming from queue '{self.queue_name}'")
	
    def stop_consuming(self):
        if not self.consumer_tag:
            return

        try:
            self.channel.stop_consuming(consumer_tag=self.consumer_tag)
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError(f"Connection lost while stop consuming from '{self.queue_name}'")
        except pika.exceptions.AMQPError:
            raise MessageMiddlewareMessageError(f"Error while stop consuming from '{self.queue_name}'")

        self.consumer_tag = None

    def send(self, message):
        try:
            self.channel.basic_publish(
                exchange='',
                body=message,
                routing_key=self.queue_name
            )
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError(f"Connection lost while sending to '{self.queue_name}'")
        except pika.exceptions.AMQPError:
            raise MessageMiddlewareMessageError(f"Error while sending to '{self.queue_name}'")

    def close(self):
        try:
            self.connection.close()
        except pika.exceptions.AMQPError:
            # close no idempotente -> si cierro algo ya cerrado explota
            raise MessageMiddlewareCloseError("Error while closing connection")

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        pass
