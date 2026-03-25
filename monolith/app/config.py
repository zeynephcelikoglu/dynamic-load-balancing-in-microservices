import os

class Config:
    SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret")
    SQLALCHEMY_DATABASE_URI = os.getenv("DATABASE_URL", "sqlite:///local.db")
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    WTF_CSRF_TIME_LIMIT = None

    DEFAULT_TAX_RATE = float(os.getenv("DEFAULT_TAX_RATE", "0.10"))
    DEFAULT_SHIPPING_FLAT = float(os.getenv("DEFAULT_SHIPPING_FLAT", "99.0"))
