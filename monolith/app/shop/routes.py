from flask import Blueprint, render_template, request
from ..models import Product, Category

bp = Blueprint("shop", __name__)

@bp.route("/")
def index():
    q = request.args.get("q")
    category_id = request.args.get("category_id", type=int)
    query = Product.query.filter_by(is_active=True)
    if q:
        query = query.filter(Product.name.ilike(f"%{q}%"))
    if category_id:
        query = query.filter_by(category_id=category_id)
    products = query.order_by(Product.created_at.desc()).all()
    categories = Category.query.order_by(Category.name).all()
    return render_template("shop/index.html", products=products, categories=categories, q=q, category_id=category_id)

@bp.route("/product/<int:pid>")
def product_detail(pid):
    p = Product.query.get_or_404(pid)
    return render_template("shop/product_detail.html", p=p)
