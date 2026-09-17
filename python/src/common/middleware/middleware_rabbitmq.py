import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        # TODO: manejo de errores
        connection = pika.BlockingConnection(pika.ConnectionParameters(host))
        channel = connection.channel()
        channel.queue_declare(queue=queue_name, durable=True)

        self.connection = connection
        self.channel = channel
        self.queue_name = queue_name
        self.consumer_tag = None
        
    #Comienza a escuchar a la cola/exchange e invoca a on_message_callback tras
	#cada mensaje de datos o de control con el cuerpo del mensaje.
	# on_message_callback tiene como parámetros:
	# message - El valor tal y como lo recibe el método send de esta clase.
	# ack - Función que al invocarse realiza ack al mensaje que se está consumiendo.
	# nack - Función que al invocarse realiza nack al mensaje que se está consumiendo. 
	#Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	#Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    def start_consuming(self, on_message_callback):
        #TODO: manejo de errores
        self.channel.basic_qos(prefetch_count=1)
        def callback(ch, method, _properties, body):

            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)

            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag)

            return on_message_callback(body, ack, nack)

        self.consumer_tag = self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback)

        self.channel.start_consuming()
	
	#Si se estaba consumiendo desde la cola/exchange, se detiene la escucha. Si
	#no se estaba consumiendo de la cola/exchange, no tiene efecto, ni levanta
	#Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    def stop_consuming(self):
        #TODO: manejo de errores
        if not self.consumer_tag:
            return

        self.channel.stop_consuming(consumer_tag=self.consumer_tag)
        self.consumer_tag = None

	#Envía un mensaje a la cola o al tópico con el que se inicializó el exchange.
	#Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	#Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    def send(self, message):
        #TODO: manejo de errores
        self.channel.basic_publish(
            exchange='',
            body=message,
            routing_key=self.queue_name
        )

    #Se desconecta de la cola o exchange al que estaba conectado.
    #Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareCloseError.
    def close(self):
        self.connection.close()

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        pass
