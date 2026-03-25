import pika, time, requests, json, psutil  # psutil ekledik

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

# Uptime Kuma URL (Sonuna &ping= ekledik)
KUMA_URL = "http://localhost:3002/api/push/FQG7y1YL1I?status=up&msg=OK&ping="

channel.exchange_declare(exchange='topic_logs', exchange_type='topic')
result = channel.queue_declare(queue='notif_queue', durable=True)
channel.queue_bind(exchange='topic_logs', queue=result.method.queue, routing_key='order.#')

# Sağlık skoru fonksiyonunu buraya da ekleyelim
def get_health_score():
    cpu = psutil.cpu_percent(interval=None)
    ram = psutil.virtual_memory().percent
    return round((cpu * 0.5) + (ram * 0.5), 2) # Notif hafif iştir, 50/50 ağırlık verdik

def callback(ch, method, properties, body):
    # Skor hesapla ve gönder
    score = get_health_score()
    try: 
        requests.get(KUMA_URL + str(score), timeout=2)
    except: 
        pass

    data = json.loads(body)
    print(f"\n[NOTIF SERVICE] İşlem Başarılı! Sağlık Skoru: {score}")
    print(f" > Kullanıcıya Mail Gitti.")
    
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(queue='notif_queue', on_message_callback=callback)
print(' [*] Bildirim Servisi Dinliyor...')
channel.start_consuming()