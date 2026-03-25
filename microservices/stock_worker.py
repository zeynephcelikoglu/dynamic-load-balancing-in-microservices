import pika, psutil, time, requests, redis, json

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

# Uptime Kuma PUSH URL (Kuma'da 'Stock Service' için oluşturduğun URL)
KUMA_URL = "http://localhost:3002/api/push/nTpBEiBntB?status=up&msg=OK&ping="

channel.exchange_declare(exchange='topic_logs', exchange_type='topic')
result = channel.queue_declare(queue='stock_queue', durable=True)
channel.queue_bind(exchange='topic_logs', queue=result.method.queue, routing_key='stok.#')

def callback(ch, method, properties, body):
    cpu_now = psutil.cpu_percent(interval=None)
    # DİKKAT: Burada sunumda şov yapmak için cpu_limit'i düşük tutabilirsin (Örn: 50)
    limit = 80 
    
    try: requests.get(KUMA_URL + str(cpu_now), timeout=2)
    except: pass

    if cpu_now > limit:
        print(f" [!] KRİTİK YÜK (%{cpu_now}) - İş reddedildi, diğer işçiye aktarılıyor!")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        return

    print(f"\n[STOCK SERVICE] Stok Güncelleniyor...")
    # SUNUM TÜYOSU: Burayı 5 yaparsan sistem hemen şişer ve scaling'i gösterirsin
    time.sleep(8) 
    print(f" [OK] Stok düşüldü. Mevcut CPU: %{cpu_now}")
    
    r.incr('basarili_stok')
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='stock_queue', on_message_callback=callback)
print(' [*] Stok Servisi Dinliyor (Scaling Odaklı)...')
channel.start_consuming()