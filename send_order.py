import pika
import random
import time

# 1. Bağlantı Ayarları
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

# 2. Exchange Tanımlama
# Consumer tarafında tanımlamış olsak da, Producer tarafında da 
# tanımlamak her zaman en güvenli yoldur.
channel.exchange_declare(exchange='topic_logs', exchange_type='topic')

# 3. İş Havuzu (İş Mantığı Katmanı)
# Format: <servis>.<yük>.<öncelik>
is_havuzu = [
    "stok.hafif.normal",   # Standart iş
    "stok.agir.normal",    # İşçiyi yoracak iş
    "stok.agir.kritik",    # Hem yorucu hem acil
    "odeme.hafif.kritik",  # Hafif ama acil
    "bilinmeyen.tip.hata"  # DLX'i tetikleyecek, listede olmayan tip
]

print(" [x] İş Üretici Başlatıldı. Durdurmak için CTRL+C")

try:
    while True:
        # Rastgele bir iş tipi seç
        routing_key = random.choice(is_havuzu)
        
        # Mesaj içeriği
        message = f"İş Detayı: {routing_key} - ID: {random.randint(1000, 9999)}"
        
        # 4. Yayınlama (Publish)
        channel.basic_publish(
            exchange='topic_logs',
            routing_key=routing_key,
            body=message
        )
        
        print(f" [V] Gönderildi: {routing_key}")
        
        # İşlerin sisteme akış hızı (Örn: her 1 saniyede bir yeni iş)
        time.sleep(1)

except KeyboardInterrupt:
    print("\n [!] Üretici durduruluyor...")
    connection.close()