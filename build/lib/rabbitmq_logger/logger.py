import pika
import json


class RabbitMQLogger:
    def __init__(self, login='',password='', host='localhost', port=5672, virtual_host='/'):
        self.credentials = pika.PlainCredentials(username=login, password=password)
        self.parameters = pika.ConnectionParameters(
            host=host, port=port,
            virtual_host=virtual_host,
            credentials=self.credentials
        )
        self.connection = pika.BlockingConnection(self.parameters)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='logs', durable=True)
    def publish(self, dict_message):
        message = json.dumps(dict_message)
        self.channel.basic_publish(
            exchange='',
            routing_key='logs',
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent
            ))
    def close(self):
        self.connection.close()