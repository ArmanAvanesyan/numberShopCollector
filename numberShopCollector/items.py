import scrapy


class MobileItem(scrapy.Item):
    mobile_number = scrapy.Field()
    mnc = scrapy.Field()
    msn = scrapy.Field()
    status = scrapy.Field()
    shop = scrapy.Field()
