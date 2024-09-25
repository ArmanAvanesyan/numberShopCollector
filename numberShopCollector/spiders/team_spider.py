import scrapy
from numberShopCollector.numberShopCollector.loaders import TeamItemLoader
from numberShopCollector.numberShopCollector.selectors import TeamSelectors


class TeamSpider(scrapy.Spider):
    name = "team"
    allowed_domains = ["www.telecomarmenia.am"]
    start_urls = ["https://www.telecomarmenia.am/eshop/en/numbers/"]

    def parse(self, response):
        for product in response.css(TeamSelectors.PRODUCT):
            loader = TeamItemLoader(selector=product)
            loader.add_css('mobile_number', TeamSelectors.MOBILE_NUMBER)
            yield loader.load_item()

        next_page = response.css(TeamSelectors.NEXT_PAGE).get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)
