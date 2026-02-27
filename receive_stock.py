import pika
import psutil
import time

# 1. Bağlantı Ayarları
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

# 2. DLX Yapılandırması
channel.exchange_declare(exchange='dead_letter_exchange', exchange_type='fanout')
channel.queue_declare(queue='backup_queue', durable=True)
channel.queue_bind(exchange='dead_letter_exchange', queue='backup_queue')

# 3. Ana Exchange ve Kuyruk
channel.exchange_declare(exchange='topic_logs', exchange_type='topic')
args = {'x-dead-letter-exchange': 'dead_letter_exchange', 'x-message-ttl': 5000}
result = channel.queue_declare(queue='ortak_is_havuzu', durable=True, arguments=args)
queue_name = result.method.queue

# Başlangıçta her şeyi dinleyecek şekilde bağlayalım (#)
channel.queue_bind(exchange='topic_logs', queue=queue_name, routing_key='#')

def callback(ch, method, properties, body):
    routing_key = method.routing_key
    
    # --- 3. AŞAMA: CANLI CPU ÖLÇÜMÜ ---
    # interval=None anlık, hızlı ölçüm sağlar.
    cpu_suan = psutil.cpu_percent(interval=None) 
    
    print(f"\n[ANALİZ] CPU: %{cpu_suan} | Gelen İş: {routing_key}")

    # SENARYO 1: ÇOK YÜKSEK YÜK (%80+) -> Sadece kritik işleri kabul et
    if cpu_suan >= 80 and ".kritik" not in routing_key:
        print(f" [!] KRİTİK SEVİYE: {routing_key} reddedildi, başkasına yönlendiriliyor...")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        time.sleep(0.5) # Diğer worker'a vakit tanı
        return

    # SENARYO 2: ORTA YÜK (%50 - %80) -> Ağır işleri reddet, hafifleri yap
    elif 50 <= cpu_suan < 80 and ".agir" in routing_key:
        print(f" [!] ORTA SEVİYE: Ağır iş ({routing_key}) pas geçildi.")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        return

    # İŞLEME SÜRECİ (Normal Şartlar veya Uygun İş)
    print(f" [x] İşleniyor: {routing_key}")
    # İşin tipine göre işlem süresi simülasyonu
    if "agir" in routing_key:
        time.sleep(1.5)
    else:
        time.sleep(0.2)
    
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Adil Dağıtım
channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue=queue_name, on_message_callback=callback)

print(' [*] Otonom İşçi (3. Aşama) Aktif. Gerçek zamanlı donanım takibi yapılıyor...')
channel.start_consuming()