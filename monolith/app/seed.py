from . import create_app
from .extensions import db
from .models import User, Category, Product, Coupon

def run():
    app = create_app()
    with app.app_context():
        db.create_all()
        if not User.query.filter_by(email="admin@example.com").first():
            u = User(email="admin@example.com", name="Admin", is_admin=True)
            u.set_password("admin123")
            db.session.add(u)
        if not Category.query.first():
            c1 = Category(name="Books", description="Books & Magazines")
            c2 = Category(name="Electronics", description="Gadgets and devices")
            db.session.add_all([c1, c2])
            db.session.flush()
            p1 = Product(sku="BK-001", name="Flask Web Dev", price=699.0, stock=20, category_id=c1.id,
                         description="Build web apps with Flask", image_url="https://via.placeholder.com/300x200")
            p2 = Product(sku="EL-001", name="Wireless Mouse", price=999.0, stock=50, category_id=c2.id,
                         description="Ergonomic mouse", image_url="https://via.placeholder.com/300x200")
            db.session.add_all([p1, p2])
        if not Coupon.query.filter_by(code="SAVE10").first():
            db.session.add(Coupon(code="SAVE10", discount_percent=0.10, active=True))
        db.session.commit()
        print("Seed complete.")

if __name__ == "__main__":
    run()
