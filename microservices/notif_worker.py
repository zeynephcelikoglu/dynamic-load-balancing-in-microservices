import pika, time, requests, json, psutil, sys

KUMA_URL = "http://uptime-kuma:3001/api/push/FQG7y1YL1I?status=up&msg=OK&ping="

def get_topology(worker_type):
    try:
        res = requests.post("http://host.docker.internal:5000/api/v1/register", json={"worker_type": worker_type})
        res.raise_for_status()
        return res.json()['topology']
    except Exception as e:
        print(f"[!] Controller'a (Flask) ulaşılamadı: {e}")
        sys.exit(1)

def get_health_score():
    cpu = psutil.cpu_percent(interval=None)
    ram = psutil.virtual_memory().percent
    return round((cpu * 0.5) + (ram * 0.5), 2) 

def callback(ch, method, properties, body):
    score = get_health_score()
    try: requests.get(KUMA_URL + str(score), timeout=2)
    except: pass

    data = json.loads(body)
    print(f"\n[TOKYO MERKEZ - 120ms Latency] İşlem Başarılı! Sağlık Skoru: {score}")
    print(f" > Kullanıcıya Mail Gitti.")
    
    ch.basic_ack(delivery_tag=method.delivery_tag)

def start_worker():
    topology = get_topology("notif")
    
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=topology['queue'], on_message_callback=callback)
    
    print(f" [*] Bildirim Servisi Dinliyor (Queue: {topology['queue']})...")
    channel.start_consuming()

if __name__ == "__main__":
    start_worker()