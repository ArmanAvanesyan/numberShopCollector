import scrapy
from numberShopCollector.numberShopCollector.loaders import UcomItemLoader
from numberShopCollector.numberShopCollector.selectors import UcomSelectors


class UcomSpider(scrapy.Spider):
    name = 'ucom'
    allowed_domains = ["shop.ucom.am"]
    start_urls = ["https://shop.ucom.am/en/colored-numbers.html"]

    def parse(self, response):
        for product in response.css(UcomSelectors.PRODUCT):
            loader = UcomItemLoader(selector=product)
            loader.add_css('mobile_number', UcomSelectors.MOBILE_NUMBER)
            loader.add_css('status', UcomSelectors.STATUS)
            yield loader.load_item()

        next_page = response.css(UcomSelectors.NEXT_PAGE).get()
        if next_page:
            next_page_url = response.urljoin(next_page)
            yield scrapy.Request(next_page_url, callback=self.parse)
