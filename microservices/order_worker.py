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

def get_health_score():
    cpu = psutil.cpu_percent(interval=None)
    ram = psutil.virtual_memory().percent
    return round((cpu * 0.7) + (ram * 0.3), 2)

def callback(ch, method, properties, body):
    score = get_health_score()

    data = json.loads(body)
    print(f"\n[ROME REGION - 50ms Latency] Order Processing Started - Data: {data}")
    time.sleep(1) 
    print(f"   [OK] Order successfully processed. (Health: {score})")
    
    r.incr('basarili_order')
    ch.basic_ack(delivery_tag=method.delivery_tag)

def start_worker():
    topology = get_topology("order")
    
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=topology['queue'], on_message_callback=callback)
    
    print(f" [*] Order Service Listening on Queue: {topology['queue']}...")
    channel.start_consuming()

if __name__ == "__main__":
    start_worker()