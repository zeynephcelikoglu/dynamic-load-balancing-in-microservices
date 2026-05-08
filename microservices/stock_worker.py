import pika, psutil, time, requests, redis, json, sys

r = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)

def get_topology(worker_type):
    try:
        res = requests.post("http://host.docker.internal:5000/api/v1/register", json={"worker_type": worker_type})
        res.raise_for_status()
        return res.json()['topology']
    except Exception as e:
        print(f"[ERROR] Unreachable Controller (Flask): {e}")
        sys.exit(1)

def callback(ch, method, properties, body):
    cpu_now = psutil.cpu_percent(interval=None)
    limit = 80 
    
    if cpu_now > limit:
        print(f" [WARNING] CRITICAL LOAD ({cpu_now}%) - Task rejected, requeuing for another worker!")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        return

    print(f"\n[LONDON REGION - 80ms Latency] Updating Stock Database...")
    time.sleep(2) 
    print(f"   [OK] Stock reduced successfully. Current CPU: {cpu_now}%")
    
    r.incr('basarili_stok')
    ch.basic_ack(delivery_tag=method.delivery_tag)

def start_worker():
    topology = get_topology("stock")
    
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=topology['queue'], on_message_callback=callback)
    
    print(f" [*] Stock Service Listening on Queue: {topology['queue']}...")
    channel.start_consuming()

if __name__ == "__main__":
    start_worker()