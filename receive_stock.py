import pika
import psutil
import time
import requests
import redis

# Bağlantı Ayarları
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

# Redis Bağlantısı
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

# DLX Yapılandırması
channel.exchange_declare(exchange='dead_letter_exchange', exchange_type='fanout')
channel.queue_declare(queue='backup_queue', durable=True)
channel.queue_bind(exchange='dead_letter_exchange', queue='backup_queue')

# Ana Exchange ve Kuyruk
channel.exchange_declare(exchange='topic_logs', exchange_type='topic')
args = {'x-dead-letter-exchange': 'dead_letter_exchange', 'x-message-ttl': 5000}
result = channel.queue_declare(queue='ortak_is_havuzu', durable=True, arguments=args)
queue_name = result.method.queue
channel.queue_bind(exchange='topic_logs', queue=queue_name, routing_key='#')

def kuma_push_cpu(cpu_degeri):
    url = f"http://localhost:3002/api/push/d1L4L9an1v?status=up&msg=CPU_Yuku&ping={cpu_degeri}"
    try:
        requests.get(url, timeout=2)
    except:
        pass

def kuma_push_success(count):
    url = f"http://localhost:3002/api/push/iXwVwdIaef?status=up&msg=OK&ping={count}"
    try:
        requests.get(url, timeout=2)
    except:
        pass

def callback(ch, method, properties, body):
    routing_key = method.routing_key
    cpu_suan = psutil.cpu_percent(interval=None) 
    
    # Uptime Kuma güncelleme
    kuma_push_cpu(cpu_suan)

    print(f"\n[ANALİZ] CPU: %{cpu_suan} | Gelen İş: {routing_key}")

    # SENARYO 1: ÇOK YÜKSEK YÜK (%80+) -> Sadece kritik işleri kabul et
    if cpu_suan >= 80 and ".kritik" not in routing_key:
        print(f" [!] KRİTİK SEVİYE: {routing_key} reddedildi.")
        r.incr('reddedilen_is') # Redis sayacını artır
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        time.sleep(0.5)
        return

    # SENARYO 2: ORTA YÜK (%50 - %80) -> Ağır işleri reddet
    elif 50 <= cpu_suan < 80 and ".agir" in routing_key:
        print(f" [!] ORTA SEVİYE: Ağır iş ({routing_key}) pas geçildi.")
        r.incr('pas_gecilen_is') # Yeni sayaç
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        return

    # İşleme süreci
    print(f" [x] İşleniyor: {routing_key}")
    if "agir" in routing_key:
        time.sleep(1.5)
    else:
        time.sleep(0.2)
    
    # Başarıyı Redise kaydet
    r.incr('basarili_is')

    guncel_basari = r.get('basarili_is')
    kuma_push_success(guncel_basari) # Kumadaki başarı grafiğini güncelle
    
    # İstatistikleri getir
    basari = r.get('basarili_is') or 0
    red = r.get('reddedilen_is') or 0
    print(f"--- REDIS İSTATİSTİK --- Toplam Başarı: {basari} | Toplam Red: {red}")
    
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Adil Dağıtım
channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue=queue_name, on_message_callback=callback)

print(' [*] Otonom İşçi (Redis Destekli) Aktif...')
channel.start_consuming()