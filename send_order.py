import pika
import random
import time

# Bağlantı Ayarları
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

# Exchange Tanımlama
# Producer tarafında da tanımlamak güvenli
channel.exchange_declare(exchange='topic_logs', exchange_type='topic')

# İş Havuzu (İş Mantığı Katmanı)
# Format: <servis>.<yük>.<öncelik>
is_havuzu = [
    "stok.hafif.normal",   # Standart iş
    "stok.agir.normal",    # İşçiyi yoracak iş
    "stok.agir.kritik",    # Hem yorucu hem acil
    "odeme.hafif.kritik",  # Hafif ama acil
    "bilinmeyen.tip.hata"  # DLX'i tetikleyecek listede olmayan tip
]

print(" [x] İş Üretici Başlatıldı. Durdurmak için CTRL+C")

try:
    while True:
        # Rastgele bir iş tipi seç
        routing_key = random.choice(is_havuzu)
        
        # Mesaj içeriği
        message = f"İş Detayı: {routing_key} - ID: {random.randint(1000, 9999)}"
        
        # Publish
        channel.basic_publish(
            exchange='topic_logs',
            routing_key=routing_key,
            body=message
        )
        
        print(f" [V] Gönderildi: {routing_key}")
        
        # İşlerin sisteme akış hızı 
        time.sleep(1)

except KeyboardInterrupt:
    print("\n [!] Üretici durduruluyor...")
    connection.close()