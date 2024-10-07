import psycopg2
from psycopg2 import sql
from scrapy.utils.project import get_project_settings


class PostgresDB:
    def __init__(self):
        settings = get_project_settings()
        self.connection = psycopg2.connect(
            host=settings.get('POSTGRESQL')['host'],
            port=settings.get('POSTGRESQL')['port'],
            database=settings.get('POSTGRESQL')['database'],
            user=settings.get('POSTGRESQL')['user'],
            password=settings.get('POSTGRESQL')['password']
        )
        self.connection.autocommit = False
        self.cursor = self.connection.cursor()

    def fetch_new_shop_results(self):
        """
        Fetch records from shop_results where is_new=True or status_change=True.
        Returns a list of tuples: (id, mobile_number, status, is_new, status_change)
        """
        query = sql.SQL("""
            SELECT id, mobile_number, status, is_new, status_change
            FROM shop_results
            WHERE is_new = TRUE OR status_change = TRUE
        """)
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def update_mobile_numbers(self, mobile_number, shop_status):
    """
    Update the shop_status in mobile_numbers table based on mobile_number.
    Set is_active to False if shop_status is 0.
    """
    query = sql.SQL("""
        UPDATE mobile_numbers
        SET shop_status = %s,
            is_active = CASE WHEN %s = 0 THEN FALSE ELSE is_active END
        WHERE number = %s
    """)
    self.cursor.execute(query, (shop_status, shop_status, mobile_number))


    def mark_shop_result_processed(self, result_id, is_new=False, status_change=False):
        """
        Mark the shop_result as processed by setting is_new and status_change flags.
        """
        query = sql.SQL("""
            UPDATE shop_results
            SET is_new = %s, status_change = %s
            WHERE id = %s
        """)
        self.cursor.execute(query, (is_new, status_change, result_id))

    def commit(self):
        """Commit the current transaction."""
        self.connection.commit()

    def rollback(self):
        """Rollback the current transaction."""
        self.connection.rollback()

    def close(self):
        """Close the cursor and connection."""
        self.cursor.close()
        self.connection.close()
