import pika

# RabbitMQ bağlantısı
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# ana yönlendiriciyi oluşturma
channel.exchange_declare(exchange='ana_dagitici', exchange_type='topic')

print("Altyapı hazır: RabbitMQ bağlantısı kuruldu ve Exchange oluşturuldu.")
connection.close()