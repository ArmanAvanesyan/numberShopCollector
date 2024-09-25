import scrapy
from numberShopCollector.numberShopCollector.db_utils import PostgresDB
from numberShopCollector.numberShopCollector.constants import ShopStatus
import logging


class ShopUpdaterSpider(scrapy.Spider):
    name = "shopUpdater"
    allowed_domains = ["postgresql"]
    start_urls = []

    def __init__(self, *args, **kwargs):
        super(ShopUpdaterSpider, self).__init__(*args, **kwargs)
        self.db = PostgresDB()

    def start_requests(self):
        """
        Initiates the spider by making a dummy request.
        Since this spider interacts directly with the database,
        the URL is irrelevant.
        """
        yield scrapy.Request(url='http://localhost/', callback=self.parse)

    def parse(self, response):
        """
        Core logic of the spider:
        - Fetch records that are new or have changed status.
        - Update mobile_numbers table accordingly.
        - Mark records as processed in shop_results.
        """
        try:
            records = self.db.fetch_new_shop_results()
            if not records:
                self.logger.info("No new or changed shop results to process.")
                return

            for record in records:
                result_id, mobile_number, status, is_new, status_change = record

                if is_new:
                    shop_status = status
                    update_reason = "is_new=True"
                elif status_change:
                    shop_status = status
                    update_reason = "status_change=True"
                else:
                    continue

                self.db.update_mobile_numbers(mobile_number, shop_status)
                self.logger.info(
                    f"Updated mobile_number {mobile_number} with status '{shop_status}' ({update_reason})"
                )

                if is_new:
                    self.db.mark_shop_result_processed(result_id, is_new=False, status_change=False)
                elif status_change:
                    self.db.mark_shop_result_processed(result_id, is_new=is_new, status_change=False)

            self.db.commit()
            self.logger.info("All updates committed successfully.")

        except Exception as e:
            self.db.rollback()
            self.logger.error(f"An error occurred during processing: {e}")
            raise e

        finally:
            self.db.close()
            self.logger.info("Database connection closed.")

        self.logger.info("shopUpdater spider finished processing.")
