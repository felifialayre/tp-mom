import pika

from .middleware import (
    MessageMiddlewareCloseError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareQueue,
)


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        connection = None
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host))
            channel = connection.channel()
            channel.queue_declare(queue=queue_name, durable=True)
        except pika.exceptions.AMQPConnectionError as e:
            if connection and connection.is_open:
                connection.close()
            raise MessageMiddlewareDisconnectedError(f"Not able to connect to {host}") from e
        except pika.exceptions.AMQPError as e:
            if connection and connection.is_open:
                connection.close()
            raise MessageMiddlewareMessageError(f"Not able to declare queue: '{queue_name}'") from e

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
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Connection lost while consuming from {self.queue_name}") from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(f"Error while consuming from queue '{self.queue_name}'") from e
        finally:
            self.consumer_tag = None
	
    def stop_consuming(self):
        if not self.consumer_tag:
            return

        try:
            self.channel.stop_consuming(consumer_tag=self.consumer_tag)
        except pika.exceptions.AMQPConnectionError as e: 
            raise MessageMiddlewareDisconnectedError(f"Connection lost while stop consuming from '{self.queue_name}'") from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(f"Error while stop consuming from '{self.queue_name}'") from e

        self.consumer_tag = None

    def send(self, message):
        try:
            self.channel.basic_publish(
                exchange='',
                body=message,
                routing_key=self.queue_name
            )
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Connection lost while sending to '{self.queue_name}'") from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(f"Error while sending to '{self.queue_name}'") from e

    def close(self):
        try:
            self.connection.close()
        except pika.exceptions.AMQPError as e:
            # close no idempotente -> si cierro algo ya cerrado explota
            raise MessageMiddlewareCloseError("Error while closing connection") from e


"""--------------------------------------------------------------------------------------------------------------------"""
class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        connection = None
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host))
            channel = connection.channel()
            channel.exchange_declare(exchange=exchange_name, exchange_type='direct')
            result = channel.queue_declare(exclusive=True, queue='')
        except pika.exceptions.AMQPConnectionError as e:
            if connection and connection.is_open:
                connection.close()
            raise MessageMiddlewareDisconnectedError(f"Not able to connect to {host}") from e
        except pika.exceptions.AMQPError as e:
            if connection and connection.is_open:
                connection.close()
            raise MessageMiddlewareMessageError("Not able to declare queue") from e

        self.routing_keys = routing_keys
        self.connection = connection
        self.channel = channel
        self.queue_name = result.method.queue
        self.exchange_name = exchange_name
        self.consumer_tag = None


    def start_consuming(self, on_message_callback):
        try:
            for key in self.routing_keys:
                self.channel.queue_bind(queue=self.queue_name,
                                        exchange=self.exchange_name,
                                        routing_key=key)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError("Could not bind to routing keys") from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError("Error binding to routing keys") from e

        def callback(ch, method, _properties, body):

            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)

            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag)

            return on_message_callback(body, ack, nack)

        try:
            self.consumer_tag = self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback)
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Connection lost while consuming from {self.queue_name}") from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(f"Error while consuming from queue '{self.queue_name}'") from e
        finally:
            self.consumer_tag = None
	
    def stop_consuming(self):
        if not self.consumer_tag:
            return
        try:
            self.channel.stop_consuming(consumer_tag=self.consumer_tag)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Connection lost while stop consuming from '{self.queue_name}'") from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(f"Error while stop consuming from '{self.queue_name}'") from e

        self.consumer_tag = None

    def send(self, message):
        try:
            for key in self.routing_keys:
                self.channel.basic_publish(
                    exchange=self.exchange_name,
                    body=message,
                    routing_key=key,
                    properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent)
                )
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Connection lost while sending to '{self.queue_name}'") from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(f"Error while sending to '{self.queue_name}'") from e

    def close(self):
        try:
            self.connection.close()
        except pika.exceptions.AMQPError as e:
            # close no idempotente -> si cierro algo ya cerrado explota
            raise MessageMiddlewareCloseError("Error while closing connection") from e
