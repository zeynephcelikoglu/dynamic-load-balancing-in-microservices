from dataclasses import dataclass
from typing import List, Optional
from flask import current_app

@dataclass
class Line:
    sku: str
    name: str
    unit_price: float
    qty: int

@dataclass
class Quote:
    subtotal: float
    discount: float
    tax: float
    shipping: float
    total: float

def apply_coupon(subtotal: float, code: Optional[str], coupon_lookup) -> float:
    if not code:
        return 0.0
    c = coupon_lookup(code)
    if not c or not c.active:
        return 0.0
    pct = max(0.0, min(c.discount_percent, 0.95))
    return round(subtotal * pct, 2)

def quote(lines: List[Line], coupon_code: Optional[str], coupon_lookup) -> Quote:
    subtotal = round(sum(l.unit_price * l.qty for l in lines), 2)
    discount = apply_coupon(subtotal, coupon_code, coupon_lookup)
    tax_rate = float(getattr(current_app.config, "DEFAULT_TAX_RATE", 0.10))
    tax = round((subtotal - discount) * tax_rate, 2) if subtotal > 0 else 0.0
    shipping = float(getattr(current_app.config, "DEFAULT_SHIPPING_FLAT", 99.0)) if subtotal > 0 else 0.0
    total = round(subtotal - discount + tax + shipping, 2)
    return Quote(subtotal, discount, tax, shipping, total)
