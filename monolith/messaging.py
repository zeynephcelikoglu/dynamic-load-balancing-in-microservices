import pika
import json

def send_order_event(routing_key, data):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    EX_NAME = 'topic_logs'
    channel.exchange_declare(exchange=EX_NAME, exchange_type='topic')
    channel.basic_publish(exchange=EX_NAME, routing_key=routing_key, body=json.dumps(data))
    connection.close()

def ensure_rabbitmq_topology(worker_type):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='127.0.0.1'))
    channel = connection.channel()
    EX_NAME = 'topic_logs'
    Q_NAME = f'{worker_type}_queue'
    RK = f'{worker_type}.#'
    channel.exchange_declare(exchange=EX_NAME, exchange_type='topic')
    channel.queue_declare(queue=Q_NAME, durable=True) # Durable önemli!
    channel.queue_bind(exchange=EX_NAME, queue=Q_NAME, routing_key=RK)
    connection.close()
    return EX_NAME, Q_NAME, RK