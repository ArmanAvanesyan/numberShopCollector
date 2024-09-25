from enum import Enum


class ShopStatus(Enum):
    AVAILABLE = 'available'
    ORDERED = 'ordered'
    SOLDOUT = 'soldout'
