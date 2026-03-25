from flask_admin.contrib.sqla import ModelView
from flask_admin import AdminIndexView, expose
from flask import redirect, url_for, request
from flask_login import current_user
from .extensions import admin, db
from .models import User, Product, Category, Coupon, Order, OrderItem, InventoryMovement, Address

class SecureModelView(ModelView):
    def is_accessible(self):
        return current_user.is_authenticated and current_user.is_admin

    def inaccessible_callback(self, name, **kwargs):
        return redirect(url_for("auth.login", next=request.url))

class MyAdminIndex(AdminIndexView):
    @expose("/")
    def index(self):
        if not (current_user.is_authenticated and current_user.is_admin):
            return redirect(url_for("auth.login", next=request.url))
        return super().index()

def register_admin(app):
    admin.init_app(app, index_view=MyAdminIndex())
    admin.add_view(SecureModelView(User, db.session))
    admin.add_view(SecureModelView(Category, db.session))
    admin.add_view(SecureModelView(Product, db.session))
    admin.add_view(SecureModelView(Coupon, db.session))
    admin.add_view(SecureModelView(Order, db.session))
    admin.add_view(SecureModelView(OrderItem, db.session))
    admin.add_view(SecureModelView(Address, db.session))
    admin.add_view(SecureModelView(InventoryMovement, db.session))
