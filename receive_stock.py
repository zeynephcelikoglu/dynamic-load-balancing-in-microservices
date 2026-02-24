import pika
import psutil
import time

# 1. Bağlantı Ayarları
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

# 2. DLX (Yedek Plan) Tanımlama
channel.exchange_declare(exchange='dead_letter_exchange', exchange_type='fanout')
channel.queue_declare(queue='backup_queue', durable=True)
channel.queue_bind(exchange='dead_letter_exchange', queue='backup_queue')

# 3. Ana Exchange Tanımlama
channel.exchange_declare(exchange='topic_logs', exchange_type='topic')

# 4. Ana Kuyruğu DLX ile Bağlama
# Eğer bu kuyruktaki mesajlar sahipsiz kalırsa DLX'e gidecek
# Kuyruğa giren mesaj 5 saniye (5000 ms) içinde işlenmezse DLX'e gider
args = {
    'x-dead-letter-exchange': 'dead_letter_exchange',
    'x-message-ttl': 5000 
}
result = channel.queue_declare(queue='ortak_is_havuzu', durable=True, arguments=args)
queue_name = result.method.queue

def update_bindings(cpu):
    print(f"--- Güncelleme Başlatıldı: CPU %{cpu} ---")
    
    # 1. ADIM: TAM TEMİZLİK
    # Sadece '#' değil, olabilecek tüm ihtimalleri tek tek siliyoruz.
    # Bu sayede kuyruk "tertemiz" hale gelir.
    try:
        channel.queue_unbind(exchange='topic_logs', queue=queue_name, routing_key='#')
        channel.queue_unbind(exchange='topic_logs', queue=queue_name, routing_key='*.*.kritik')
        channel.queue_unbind(exchange='topic_logs', queue=queue_name, routing_key='stok.#')
        channel.queue_unbind(exchange='topic_logs', queue=queue_name, routing_key='odeme.#')
    except:
        pass # Eğer bağ yoksa hata vermesin diye
    
    # 2. ADIM: YENİ KURALLARI YAZ
    if cpu < 50:
        # Sadece bu kural varken her şeyi alır
        channel.queue_bind(exchange='topic_logs', queue=queue_name, routing_key='#')
        print(" [STATUS] CPU Düşük: Her şey serbest (#)")
    else:
        # Sadece bu kural varken ASLA normal mesaj alamaz
        channel.queue_bind(exchange='topic_logs', queue=queue_name, routing_key='*.*.kritik')
        print(" [STATUS] CPU YÜKSEK: SADECE KRİTİK İŞLER!")

def callback(ch, method, properties, body):
    routing_key = method.routing_key
    cpu_suan = test_cpu_degeri  # Test ettiğimiz CPU değeri

    # AKILLI KONTROL: Eğer CPU yüksekse ve gelen iş kritik değilse REDDET
    if cpu_suan >= 80 and ".kritik" not in routing_key:
        print(f" [!] REDDEDİLDİ: {routing_key} (CPU Yüksek, başka işçiye gitsin)")
        # requeue=True diyerek mesajı kuyruğa geri bırakıyoruz. 
        # Böylece boş olan Worker 2 bu mesajı alabilir.
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        # 0.5 saniye bekle ki RabbitMQ mesajı hemen sana geri sormasın, 
        # diğer işçiye sormaya vakit bulsun.
        time.sleep(0.5)
        return

    # Normal İşleme Süreci
    print(f" [x] İşleniyor: {routing_key} | İçerik: {body.decode()}")
    if "agir" in routing_key:
        time.sleep(2)
    else:
        time.sleep(0.2)
    
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Başlangıç Ayarı
cpu_usage = psutil.cpu_percent()

test_cpu_degeri = 20 # Burayı 90 yaparsan "Yorgun", 20 yaparsan "Boş" olur.

if test_cpu_degeri is not None:
    cpu_usage = test_cpu_degeri
else:
    cpu_usage = psutil.cpu_percent()

update_bindings(test_cpu_degeri)

# Adil dağıtım için (Prefetch): Bir işçi bir işi bitirmeden yenisini almasın
channel.basic_qos(prefetch_count=1)

channel.basic_consume(queue=queue_name, on_message_callback=callback)

print(' [*] Akıllı İşçi çalışıyor. Çıkış için CTRL+C')
channel.start_consuming()