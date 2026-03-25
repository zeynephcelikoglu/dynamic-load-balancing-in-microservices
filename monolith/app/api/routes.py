from flask import Blueprint, jsonify, request, session
from flask_login import login_required, current_user
from ..models import Product, Category, Coupon, Order, OrderItem
from ..utils.pricing import Line, quote

bp = Blueprint("api", __name__)

def _coupon_lookup(code):
    return Coupon.query.filter_by(code=code).first()

@bp.get("/health")
def health():
    return jsonify({"status": "ok"})

@bp.get("/products")
def api_products():
    q = request.args.get("q")
    query = Product.query.filter_by(is_active=True)
    if q:
        query = query.filter(Product.name.ilike(f"%{q}%"))
    products = query.all()
    return jsonify([{
        "id": p.id, "sku": p.sku, "name": p.name, "price": p.price,
        "stock": p.stock, "category_id": p.category_id, "image_url": p.image_url
    } for p in products])

@bp.get("/categories")
def api_categories():
    cats = Category.query.all()
    return jsonify([{"id": c.id, "name": c.name} for c in cats])

@bp.post("/cart/add")
def api_cart_add():
    pid = request.json.get("product_id")
    qty = int(request.json.get("qty", 1))
    cart = session.setdefault("cart", {})
    cart[str(pid)] = cart.get(str(pid), 0) + qty
    session.modified = True
    return jsonify({"ok": True, "cart": cart})

@bp.post("/quote")
def api_quote():
    data = request.json or {}
    lines = [Line(**l) for l in data.get("lines", [])]
    coupon_code = data.get("coupon")
    q = quote(lines, coupon_code, _coupon_lookup)
    return jsonify(q.__dict__)

@bp.get("/orders/<int:order_id>")
@login_required
def api_order(order_id):
    o = Order.query.get_or_404(order_id)
    if o.user_id != current_user.id and not current_user.is_admin:
        return jsonify({"error": "Forbidden"}), 403
    return jsonify({
        "id": o.id, "status": o.status, "subtotal": o.subtotal, "discount": o.discount,
        "tax": o.tax, "shipping": o.shipping, "total": o.total,
        "items": [{
            "product_id": i.product_id, "name": i.product_name, "sku": i.sku,
            "unit_price": i.unit_price, "qty": i.quantity, "line_total": i.line_total
        } for i in o.items]
    })
