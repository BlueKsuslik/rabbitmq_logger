import pika
import json
import logging
import traceback
import datetime
from os import environ
from flask import request
from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any
from contextlib import contextmanager


class RabbitMQLogger:
    def __init__(self, login: str = '', password: str = '', host: str = 'localhost',
                 port: int = 5672, virtual_host: str = '/', queue: str = ''):
        self.credentials = pika.PlainCredentials(username=login, password=password)
        self.parameters = pika.ConnectionParameters(
            host=host,
            port=port,
            virtual_host=virtual_host,
            credentials=self.credentials,
            connection_attempts=3,
            retry_delay=5
        )
        self.queue = queue
        self._connection = None
        self._channel = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def connect(self):
        """Устанавливает соединение с RabbitMQ"""
        try:
            self._connection = pika.BlockingConnection(self.parameters)
            self._channel = self._connection.channel()
            self._channel.queue_declare(queue=self.queue, durable=True)
        except pika.exceptions.AMQPError as e:
            logging.error(f"RabbitMQ connection error: {e}")
            raise

    def publish(self, dict_message: Dict[str, Any], headers: Optional[Dict[str, str]] = None):
        """Публикует сообщение в очередь RabbitMQ с заголовками"""
        try:
            message = json.dumps(dict_message, default=str)
            properties = pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type='application/json',
                headers=headers or {}
            )
            self._channel.basic_publish(
                exchange='',
                routing_key=self.queue,
                body=message,
                properties=properties
            )
        except (pika.exceptions.AMQPError, json.JSONEncodeError) as e:
            logging.error(f"Failed to publish message: {e}")
            raise

    def close(self):
        """Закрывает соединение с RabbitMQ"""
        if self._connection and not self._connection.is_closed:
            self._connection.close()
