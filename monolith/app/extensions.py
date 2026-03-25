from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from flask_login import LoginManager
from flask_wtf.csrf import CSRFProtect
from flask_bcrypt import Bcrypt
from flask_admin import Admin

db = SQLAlchemy()
migrate = Migrate()
login_manager = LoginManager()
csrf = CSRFProtect()
bcrypt = Bcrypt()
admin = Admin(name="E-Commerce Admin")

@login_manager.user_loader
def load_user(user_id):
    from .models import User
    return User.query.get(int(user_id))

login_manager.login_view = "auth.login"
