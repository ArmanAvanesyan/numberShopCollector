from scrapy.exceptions import DropItem
import psycopg2
from psycopg2 import extras
from scrapy.utils.project import get_project_settings
import logging
from datetime import datetime


class DuplicateFilterPipeline:
    """Pipeline to filter out duplicate mobile numbers for ucom_spider."""

    def __init__(self):
        self.seen = set()

    def process_item(self, item, spider):
        if item['shop'] == 'ucom':
            if item['mobile_number'] in self.seen:
                raise DropItem(f"Duplicate item found: {item['mobile_number']}")
            else:
                self.seen.add(item['mobile_number'])
        return item


class ValidationPipeline:
    """Pipeline to validate item fields."""

    def process_item(self, item, spider):
        required_fields = ['mobile_number', 'mnc', 'msn', 'status', 'shop']
        for field in required_fields:
            if field not in item or not item[field]:
                raise DropItem(f"Missing required field: {field}")

        if len(item['mobile_number']) != 8:
            raise DropItem(f"Invalid mobile number length: {item['mobile_number']}")

        valid_statuses = {
            'ucom': ['available', 'ordered', 'soldout'],
            'team': ['available']
        }
        if item['status'] not in valid_statuses.get(item['shop'], []):
            raise DropItem(f"Invalid status '{item['status']}' for shop '{item['shop']}'")

        return item


class DatabasePipeline:
    """Pipeline to store items in PostgreSQL with enhanced update logic."""

    def __init__(self, batch_size=100):
        self.batch_size = batch_size
        self.items = []

        settings = get_project_settings()
        self.connection = psycopg2.connect(
            host=settings.get('POSTGRESQL')['host'],
            port=settings.get('POSTGRESQL')['port'],
            database=settings.get('POSTGRESQL')['database'],
            user=settings.get('POSTGRESQL')['user'],
            password=settings.get('POSTGRESQL')['password']
        )
        self.cursor = self.connection.cursor()
        logging.info("Database connection established.")

    @classmethod
    def from_crawler(cls, crawler):
        batch_size = crawler.settings.getint('DB_PIPELINE_BATCH_SIZE', 100)
        return cls(batch_size=batch_size)

    def process_item(self, item, spider):
        item['spider_name'] = spider.name
        item['last_seen'] = datetime.now()

        self.items.append(item)

        if len(self.items) >= self.batch_size:
            self._process_batch()

        return item

    def _process_batch(self):
        try:
            keys = [(item['mobile_number'], item['spider_name']) for item in self.items]

            query = """
                SELECT mobile_number, spider_name, status
                FROM shop_results
                WHERE (mobile_number, spider_name) IN %s
            """
            extras.execute_values(
                self.cursor,
                query,
                keys,
                template=None,
                page_size=100
            )
            existing_records = self.cursor.fetchall()

            existing_dict = {(record[0], record[1]): record[2] for record in existing_records}

            to_insert = []
            to_update = []

            for item in self.items:
                key = (item['mobile_number'], item['spider_name'])
                new_status = item['status']

                if key in existing_dict:
                    existing_status = existing_dict[key]
                    is_new = False
                    status_change = new_status != existing_status

                    to_update.append((
                        new_status,
                        is_new,
                        status_change,
                        item['last_seen'],
                        item['mobile_number'],
                        item['spider_name']
                    ))
                else:
                    is_new = True
                    status_change = False

                    to_insert.append((
                        item['mobile_number'],
                        item['mnc'],
                        item['msn'],
                        item['status'],
                        item['spider_name'],
                        is_new,
                        item['last_seen'],
                        status_change
                    ))

            if to_insert:
                insert_query = """
                    INSERT INTO shop_results 
                    (mobile_number, mnc, msn, status, spider_name, is_new, last_seen, status_change)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                extras.execute_batch(
                    self.cursor,
                    insert_query,
                    to_insert,
                    page_size=100
                )
                logging.info(f"Inserted {len(to_insert)} new items into shop_results table.")

            if to_update:
                update_query = """
                    UPDATE shop_results
                    SET 
                        status = %s,
                        is_new = %s,
                        status_change = %s,
                        last_seen = %s
                    WHERE mobile_number = %s AND spider_name = %s
                """
                extras.execute_batch(
                    self.cursor,
                    update_query,
                    to_update,
                    page_size=100
                )
                logging.info(f"Updated {len(to_update)} existing items in shop_results table.")

            self.connection.commit()
            logging.info("Database commit successful.")

        except Exception as e:
            self.connection.rollback()
            logging.error(f"Error processing batch: {e}")
        finally:
            self.items = []

    def close_spider(self, spider):
        if self.items:
            self._process_batch()
        self.cursor.close()
        self.connection.close()
        logging.info("Database connection closed.")
