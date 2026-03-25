import pika
import json

def send_order_event(routing_key, data):
    # Senin projendeki gibi 'localhost' üzerinden bağlanıyoruz
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    
    # SENİN PROJENDEKİ EXCHANGE İSMİ: 'topic_logs'
    EXCHANGE_NAME = 'topic_logs'
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='topic')
    
    message = json.dumps(data)
    
    # Mesajı fırlatıyoruz. Etiket formatı senin projedeki gibi olsun:
    # Örn: 'stok.agir.kritik'
    channel.basic_publish(
        exchange=EXCHANGE_NAME,
        routing_key=routing_key,
        body=message
    )
    
    connection.close()
    print(f" [V] Flask'tan Mesaj Gönderildi: {routing_key}")