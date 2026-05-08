import pika, psutil, time, requests, redis, json, sys

# Uptime Kuma PUSH URL
KUMA_URL = "http://uptime-kuma:3001/api/push/FQG7y1YL1I?status=up&msg=OK&ping="
r = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)

def get_topology(worker_type):
    try:
        res = requests.post("http://host.docker.internal:5000/api/v1/register", json={"worker_type": worker_type})
        res.raise_for_status()
        return res.json()['topology']
    except Exception as e:
        print(f"[!] Controller'a (Flask) ulaşılamadı: {e}")
        sys.exit(1)

def callback(ch, method, properties, body):
    cpu_now = psutil.cpu_percent(interval=None)
    limit = 80 
    
    try: requests.get(KUMA_URL + str(cpu_now), timeout=2)
    except: pass

    if cpu_now > limit:
        print(f" [!] KRİTİK YÜK (%{cpu_now}) - İş reddedildi, diğer işçiye aktarılıyor!")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        return

    print(f"\n[LONDRA MERKEZ - 80ms Latency] Stok Güncelleniyor...")
    time.sleep(8) 
    print(f" [OK] Stok düşüldü. Mevcut CPU: %{cpu_now}")
    
    r.incr('basarili_stok')
    ch.basic_ack(delivery_tag=method.delivery_tag)

def start_worker():
    topology = get_topology("stock")
    
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=topology['queue'], on_message_callback=callback)
    
    print(f" [*] Stok Servisi Dinliyor (Queue: {topology['queue']})...")
    channel.start_consuming()

if __name__ == "__main__":
    start_worker()