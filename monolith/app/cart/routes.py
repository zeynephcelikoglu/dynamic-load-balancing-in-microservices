from flask import Blueprint, render_template, redirect, url_for, request, session, flash
from flask_login import login_required, current_user
from ..models import Product, Coupon
from ..utils.pricing import Line, quote

bp = Blueprint("cart", __name__, url_prefix="/cart")

def _cart():
    return session.setdefault("cart", {})  # {product_id: qty}

@bp.route("/")
def view_cart():
    items = []
    for pid, qty in _cart().items():
        p = Product.query.get(int(pid))
        if p:
            items.append(dict(product=p, qty=qty))
    return render_template("cart/view.html", items=items, coupon=session.get("coupon"))

@bp.post("/add/<int:pid>")
def add_to_cart(pid):
    p = Product.query.get_or_404(pid)
    c = _cart()
    c[str(pid)] = c.get(str(pid), 0) + 1
    session.modified = True
    flash(f"Added {p.name} to cart", "success")
    return redirect(url_for("cart.view_cart"))

@bp.post("/update/<int:pid>")
def update_item(pid):
    qty = request.form.get("qty", type=int)
    c = _cart()
    if qty <= 0:
        c.pop(str(pid), None)
    else:
        c[str(pid)] = qty
    session.modified = True
    return redirect(url_for("cart.view_cart"))

@bp.post("/apply-coupon")
def apply_coupon():
    code = request.form.get("code", "").strip().upper()
    if not code:
        session.pop("coupon", None)
        flash("Coupon removed.", "info")
    else:
        c = Coupon.query.filter_by(code=code, active=True).first()
        if not c:
            flash("Invalid coupon", "warning")
        else:
            session["coupon"] = code
            flash("Coupon applied", "success")
    session.modified = True
    return redirect(url_for("cart.view_cart"))
