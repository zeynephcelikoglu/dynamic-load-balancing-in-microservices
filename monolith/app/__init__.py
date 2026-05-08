import os
from flask import Flask
from .config import Config
from .extensions import db, migrate, login_manager, csrf, bcrypt, admin as flask_admin
from .models import User, Product, Category, Coupon, Order, OrderItem, InventoryMovement, Address
from .admin import register_admin
from dotenv import load_dotenv

def create_app():
    load_dotenv()
    app = Flask(__name__, template_folder="templates", static_folder="static")
    
    app.config.from_object(Config)
    app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv("DATABASE_URL")
    
    app.config['WTF_CSRF_ENABLED'] = False
    app.config['PRESERVE_CONTEXT_ON_EXCEPTION'] = False

    db.init_app(app)
    migrate.init_app(app, db)
    login_manager.init_app(app)
    csrf.init_app(app)
    bcrypt.init_app(app)

    from .auth.routes import bp as auth_bp
    from .shop.routes import bp as shop_bp
    from .cart.routes import bp as cart_bp
    from .orders.routes import bp as orders_bp
    from .api.routes import bp as api_bp

    app.register_blueprint(auth_bp)
    app.register_blueprint(shop_bp)
    app.register_blueprint(cart_bp)
    app.register_blueprint(orders_bp)
    app.register_blueprint(api_bp, url_prefix="/api/v1")

    register_admin(app)

    @app.shell_context_processor
    def make_shell_ctx():
        return dict(db=db, User=User, Product=Product, Category=Category, Coupon=Coupon,
                    Order=Order, OrderItem=OrderItem, InventoryMovement=InventoryMovement, Address=Address)
    return app
