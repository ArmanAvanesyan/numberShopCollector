from scrapy.loader import ItemLoader
from scrapy.loader.processors import TakeFirst, MapCompose
from numberShopCollector.numberShopCollector.items import MobileItem


def clean_number(number):
    """Remove spaces and leading zeros from the mobile number."""
    return number.replace(" ", "").lstrip('0')


def extract_status(class_attr):
    if 'available' in class_attr:
        return 'available'
    elif 'ordered' in class_attr:
        return 'ordered'
    elif 'soldout' in class_attr:
        return 'soldout'
    else:
        return 'unknown'


class UcomItemLoader(ItemLoader):
    default_item_class = MobileItem
    default_output_processor = TakeFirst()

    mobile_number_in = MapCompose(str.strip)
    status_in = MapCompose(str.lower, extract_status)

    def load_item(self):
        item = super().load_item()
        mobile_number = item.get('mobile_number', '')
        item['mnc'] = mobile_number[:2]
        item['msn'] = mobile_number[2:]
        item['shop'] = 'ucom'
        return item


class TeamItemLoader(ItemLoader):
    default_item_class = MobileItem
    default_output_processor = TakeFirst()

    mobile_number_in = MapCompose(clean_number, str.strip)

    def load_item(self):
        item = super().load_item()
        mobile_number = item.get('mobile_number', '')
        item['mnc'] = mobile_number[:2]
        item['msn'] = mobile_number[2:]
        item['shop'] = 'team'
        item['status'] = 'available'
        return item
