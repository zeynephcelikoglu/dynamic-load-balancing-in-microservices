import pika

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

# DLX exchange ve kuyruğunu tanımlanmıştı sadece o kuyruğu dinleniyor
channel.queue_declare(queue='backup_queue', durable=True)

print(' [!] DLX Monitor: Sahipsiz/Hatalı mesajlar bekleniyor. Çıkış: CTRL+C')

def callback(ch, method, properties, body):
    print(f" [ALERT] Sahipsiz Mesaj Yakalandı: {body.decode()}")
    # Mesajı aldığını onayla
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(queue='backup_queue', on_message_callback=callback)
channel.start_consuming()