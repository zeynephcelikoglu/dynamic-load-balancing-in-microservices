from flask import Blueprint, render_template, redirect, url_for, request, session, flash
from flask_login import login_required, current_user
from ..extensions import db
from ..models import Product, Order, OrderItem, Coupon
from ..utils.pricing import Line, quote
# messaging.py'den fonksiyonu içeri alıyoruz
from messaging import send_order_event 

bp = Blueprint("orders", __name__, url_prefix="/orders")

def _cart():
    return session.get("cart", {})

def _coupon_lookup(code):
    return Coupon.query.filter_by(code=code).first()

@bp.route("/checkout", methods=["GET", "POST"])
@login_required
def checkout():
    c = _cart()
    if not c:
        flash("Your cart is empty", "warning")
        return redirect(url_for("cart.view_cart"))

    lines = []
    products = {}
    for pid, qty in c.items():
        p = Product.query.get(int(pid))
        if not p or not p.is_active:
            continue
        lines.append(Line(sku=p.sku, name=p.name, unit_price=p.price, qty=qty))
        products[int(pid)] = (p, qty)

    q = quote(lines, session.get("coupon"), _coupon_lookup)

    if request.method == "POST":
        # Hızlı stok kontrolü
        for pid, (p, qty) in products.items():
            if p.stock < qty:
                flash(f"Yetersiz stok: {p.name}", "danger")
                return redirect(url_for("cart.view_cart"))

        # 1. Mesajın içeriğini (Payload) hazırla
        order_payload = {
            "user_id": current_user.id,
            "subtotal": float(q.subtotal),
            "total": float(q.total),
            "coupon_code": session.get("coupon"),
            "items": [{"pid": pid, "qty": qty, "price": float(p.price)} for pid, (p, qty) in products.items()]
        }

        # 2. MESAJLARI "TOPIC" ETİKETLERİYLE FIRLAT
        # Bu etiket 'stok_worker.py' dosyasını tetikler (Ağır iş simülasyonu)
        send_order_event('stok.agir.normal', order_payload)
        
        # Bu etiket 'order_worker.py' dosyasını tetikler (Veritabanı kaydı)
        send_order_event('order.kayit.normal', order_payload)

        # 3. Kullanıcıyı bekletmeden sepeti boşalt ve yönlendir
        session.pop("cart", None)
        session.pop("coupon", None) # Kuponu da temizleyelim
        
        flash("Siparişiniz alındı! Arka planda mikroservisler tarafından işleniyor.", "success")
        return redirect(url_for("shop.index"))

    return render_template("orders/checkout.html", quote=q, lines=lines)

@bp.route("/<int:order_id>")
@login_required
def detail(order_id):
    order = Order.query.get_or_404(order_id)
    if order.user_id != current_user.id and not current_user.is_admin:
        flash("Not authorized", "danger")
        return redirect(url_for("shop.index"))
    return render_template("orders/detail.html", order=order)