from typing import Dict

class InventoryError(Exception):
    pass

def reserve_stock(current: int, qty: int) -> int:
    if qty <= 0:
        raise InventoryError("Quantity must be positive")
    if current < qty:
        raise InventoryError("Insufficient stock")
    return current - qty

def release_stock(current: int, qty: int) -> int:
    if qty <= 0:
        raise InventoryError("Quantity must be positive")
    return current + qty
