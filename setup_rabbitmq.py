import pika

# 1. RabbitMQ bağlantısı
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# 2. Projenin ana yönlendiricisini (Exchange) oluşturma
# Tipini 'topic' yapıyoruz çünkü etiketlerle çalışacak
channel.exchange_declare(exchange='ana_dagitici', exchange_type='topic')

print("Altyapı hazır: RabbitMQ bağlantısı kuruldu ve Exchange oluşturuldu.")
connection.close()