import pika, psutil, time, requests, redis, json

# Ayarlar
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

# Uptime Kuma PUSH URL (Kuma'da 'Order Service' için oluşturduğun URL'yi buraya yapıştır)
KUMA_URL = "http://localhost:3002/api/push/C0lMSRAgA7?status=up&msg=OK&ping="

channel.exchange_declare(exchange='topic_logs', exchange_type='topic')
result = channel.queue_declare(queue='order_monitor_queue', durable=True)
channel.queue_bind(exchange='topic_logs', queue=result.method.queue, routing_key='order.#')

def get_health_score():
    cpu = psutil.cpu_percent(interval=None)
    ram = psutil.virtual_memory().percent
    return round((cpu * 0.7) + (ram * 0.3), 2)

def callback(ch, method, properties, body):
    score = get_health_score()
    try: requests.get(KUMA_URL + str(score), timeout=2)
    except: pass

    data = json.loads(body)
    print(f"\n[ORDER SERVICE] Kayıt İşlemi Başladı - Kullanıcı: {data.get('user_id')}")
    time.sleep(1) # Kayıt simülasyonu
    print(f" [OK] Sipariş DB'ye yazıldı. (Sağlık: {score})")
    
    r.incr('basarili_order')
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='order_monitor_queue', on_message_callback=callback)
print(' [*] Sipariş Kayıt Servisi Dinliyor...')
channel.start_consuming()