import pika
import json
import requests
import sys
import os
import time
from app import create_app
from app.extensions import db
from app.models import Order, OrderItem, Product, InventoryMovement
from dotenv import load_dotenv
load_dotenv()

app = create_app()

def get_topology(worker_type):
    try:
        res = requests.post("http://host.docker.internal:5000/api/v1/register", json={"worker_type": worker_type})
        res.raise_for_status()
        return res.json()['topology']
    except Exception as e:
        print(f"[ERROR] Unreachable Controller (Flask): {e}")
        sys.exit(1)

def process_order(ch, method, properties, body):
    try:
        data = json.loads(body)
        print(f"\n[ISTANBUL MAIN DB - 0ms Latency] Writing new transaction: User {data['user_id']}")
        time.sleep(1.5)
        
        with app.app_context():
            order = Order(
                user_id=data['user_id'],
                subtotal=data['subtotal'],
                total=data['total'],
                coupon_code=data.get('coupon_code')
            )
            db.session.add(order)
            db.session.flush() 

            for item in data['items']:
                p = Product.query.get(item['pid'])
                if p:
                    oi = OrderItem(
                        order_id=order.id, product_id=p.id, product_name=p.name,
                        sku=p.sku, unit_price=item['price'], quantity=item['qty'],
                        line_total=item['price'] * item['qty']
                    )
                    db.session.add(oi)
                    p.stock -= item['qty']
                    db.session.add(InventoryMovement(product_id=p.id, delta=-item['qty'], reason="purchase"))
            
            db.session.commit()
            print(f"   [OK] Transaction #{order.id} committed to database successfully!")

        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    except Exception as e:
        print(f"[ERROR] Exception occurred: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def start_worker():
    topology = get_topology("order_db_write")
    
    rabbit_host = os.getenv("RABBITMQ_HOST", "rabbitmq")
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbit_host))
    channel = connection.channel()

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=topology['queue'], on_message_callback=process_order)

    print(f" [*] Database Write Service is running on Queue: {topology['queue']}")
    channel.start_consuming()

if __name__ == "__main__":
    start_worker()