from app import create_app
from app.extensions import db
from app.models import Product, InventoryMovement, OrderItem, Order 

app = create_app()

def seed_data():
    with app.app_context():
        print("Cleaning up database records...")
        
        try:
            InventoryMovement.query.delete()
            OrderItem.query.delete()
            
            Order.query.delete()

            Product.query.delete()

            print("Injecting new professional product data...")
            products = [
                Product(
                    name="Premium Wireless Headphones",
                    sku="TECH-WH-001",
                    price=249.99,
                    stock=500,
                    description="High-fidelity audio with active noise cancellation. Shipped from London Warehouse.",
                    image_url="https://images.unsplash.com/photo-1505740420928-5e560c06d30e?w=500&q=80"
                ),
                Product(
                    name="Minimalist Smartwatch",
                    sku="TECH-SW-002",
                    price=199.50,
                    stock=300,
                    description="Track your health and stay connected. Processed via Tokyo Data Center.",
                    image_url="https://images.unsplash.com/photo-1523275335684-37898b6baf30?w=500&q=80"
                ),
                Product(
                    name="Ergonomic Developer Keyboard",
                    sku="TECH-KB-003",
                    price=129.00,
                    stock=150,
                    description="Mechanical keyboard for extreme coding sessions. DB Synced in Istanbul.",
                    image_url="https://images.unsplash.com/photo-1595225476474-87563907a212?w=500&q=80"
                ),
                Product(
                    name="Urban Running Sneakers",
                    sku="FASH-RS-004",
                    price=89.99,
                    stock=800,
                    description="Lightweight and breathable. Distributed order routing (Rome).",
                    image_url="https://images.unsplash.com/photo-1542291026-7eec264c27ff?w=500&q=80"
                )
            ]

            db.session.bulk_save_objects(products)
            db.session.commit()
            print("Database seeded successfully with high-resolution assets.")
            
        except Exception as e:
            db.session.rollback()
            print(f"Error during seeding: {e}")

if __name__ == '__main__':
    seed_data()