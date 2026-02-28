import pika
import psutil
import time
import requests
import redis
import yaml
import random

# Bağlantı Ayarları
connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
channel = connection.channel()

# Redis Bağlantısı
r = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)

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

# --- PUSH FONKSİYONLARI ---
def kuma_push_cpu(cpu_degeri):
    url = f"http://uptime-kuma:3001/api/push/nAU4iRLG0x?status=up&msg=OK&ping={cpu_degeri}"
    try:
        requests.get(url, timeout=2)
    except:
        pass

def kuma_push_success(count):
    url = f"http://uptime-kuma:3001/api/push/45EgWqf8lU?status=up&msg=OK&ping={count}"
    try:
        requests.get(url, timeout=2)
    except:
        pass

def load_config():
    try:
        with open("config.yaml", "r") as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"[!] Ayar dosyası okunamadı, varsayılan değerler kullanılıyor: {e}")
        return {"cpu_limit": 80, "medium_limit": 50}

def get_health_score():
    """CPU ve RAM değerlerini alıp ağırlıklı skor üretir."""
    cpu = psutil.cpu_percent(interval=None)
    ram = psutil.virtual_memory().percent
    
    # %70 CPU, %30 RAM ağırlığı
    score = (cpu * 0.7) + (ram * 0.3)
    return round(score, 2), cpu, ram

def callback(ch, method, properties, body):
    config = load_config()
    health_limit = config.get("cpu_limit", 80)

    # Skor hesapla
    score, cpu_now, ram_now = get_health_score()
    routing_key = method.routing_key
    
    # Dashboarda gönder
    kuma_push_cpu(score)

    print("-" * 30)
    print(f"[OTONOM ANALİZ]")
    print(f" > Sağlık Skoru: {score}")
    print(f" > Detaylar: CPU %{cpu_now} | RAM %{ram_now}")
    print(f" > Gelen İş: {routing_key}")

    # Karar Mekanizması
    if score >= health_limit and ".kritik" not in routing_key:
        print(f" [!] DURUM: KRİTİK ({score}) - İş reddedildi, kuyruğa dönüyor.")
        r.incr('reddedilen_is')
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        return
    
    # İşleme süreci
    print(f" [x] DURUM: UYGUN - İşleniyor...")

    # İşin ağırlığına göre bekleme süresi
    if ".agir" in routing_key:
        bekleme = random.uniform(1.0, 2.0)
    else:
        bekleme = random.uniform(0.1, 0.4)

    time.sleep(bekleme)
    print(f" [OK] İş {bekleme:.2f} saniyede tamamlandı.")

    # İstatistikleri Güncelle
    r.incr('basarili_is')
    guncel_basari = r.get('basarili_is')
    kuma_push_success(guncel_basari)
    basari = r.get('basarili_is') or 0
    red = r.get('reddedilen_is') or 0
    print(f"--- REDIS İSTATİSTİK --- Toplam Başarı: {basari} | Toplam Red: {red}")
    
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Adil Dağıtım
channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue=queue_name, on_message_callback=callback)

print(' [*] Otonom İşçi (Redis Destekli) Aktif...')
channel.start_consuming()