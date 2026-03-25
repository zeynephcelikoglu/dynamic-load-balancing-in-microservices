import pika
import json
import time
from app import create_app
from app.extensions import db
from app.models import Order, OrderItem, Product, InventoryMovement

app = create_app()

def process_order(ch, method, properties, body):
    data = json.loads(body)
    print(f" [.] Yeni sipariş işleniyor: User {data['user_id']}")
    
    with app.app_context():
        # Ana siparişi oluştur
        order = Order(
            user_id=data['user_id'],
            subtotal=data['subtotal'],
            total=data['total'],
            coupon_code=data['coupon_code']
        )
        db.session.add(order)
        db.session.flush() # ID almak için

        # Ürünleri ve stokları işle
        for item in data['items']:
            p = Product.query.get(item['pid'])
            oi = OrderItem(
                order_id=order.id, product_id=p.id, product_name=p.name,
                sku=p.sku, unit_price=item['price'], quantity=item['qty'],
                line_total=item['price'] * item['qty']
            )
            db.session.add(oi)
            p.stock -= item['qty']
            db.session.add(InventoryMovement(product_id=p.id, delta=-item['qty'], reason="purchase"))
        
        db.session.commit()
        print(f" [OK] Sipariş #{order.id} başarıyla kaydedildi!")
    
    ch.basic_ack(delivery_tag=method.delivery_tag)

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.exchange_declare(exchange='topic_logs', exchange_type='topic')
channel.queue_declare(queue='order_db_write_queue', durable=True)
channel.queue_bind(exchange='topic_logs', queue='order_db_write_queue', routing_key='order.#')
channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='order_db_write_queue', on_message_callback=process_order)

print(' [*] Sipariş Servisi (Worker) çalışıyor. Çıkmak için CTRL+C')
channel.start_consuming()