from datetime import datetime
import pymysql
import config


class MySQLDatabaseOther:
    def __init__(self, host, user, password, database, port=3306):
        """Initialize the MySQLDatabase class with connection parameters."""
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.connection = None

    def open_connection(self):

        try:
            self.connection = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                port=self.port
            )
            print("Connected to the MySQL database.")
        except pymysql.MySQLError as e:
            print(f"Failed to connect to MySQL: {e}")
        except Exception as e:
            print(f"Unexpected error occurred: {e}")

    def close_connection(self):
        """Close the MySQL database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
            print("MySQL connection closed.")

    def execute_query(self, query, params=None):
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                self.connection.commit()
                print("[INFO] Insert Item successfully.")
        except pymysql.MySQLError as e:
            print(f"Failed to execute query: {e}")
        except Exception as e:
            print(f"Unexpected error occurred: {e}")

    def init_table(self):
        create_table_sql = f"""
                            CREATE TABLE IF NOT EXISTS {config.STATUS_TABLE_NAME} (
                            ID INT AUTO_INCREMENT PRIMARY KEY,
                            PROJECT_NAME VARCHAR(255),
                            ISSUE_COUNTS INT,
                            ERROR_MESSAGE VARCHAR(255),
                            STATUS VARCHAR(255),
                            EXECUTION_TIME TIME,
                            CREATED DATETIME
                        );
                """
        self.execute_query(create_table_sql)

    def insert_log(self, item):
        insert_query = f"""INSERT INTO {config.STATUS_TABLE_NAME} ( PROJECT_NAME, ISSUE_COUNTS, ERROR_MESSAGE, STATUS, EXECUTION_TIME, CREATED) VALUES (
                            %(PROJECT_NAME)s, %(ISSUE_COUNTS)s, %(ERROR_MESSAGE)s, %(STATUS)s, %(EXECUTION_TIME)s, %(CREATED)s
                        );
                    """
        self.execute_query(insert_query, item)

    def insert_status_log(self, status, error=""):
        end_time = datetime.now()
        execution_time = end_time - config.created
        log_data = {
            "PROJECT_NAME": "Bayut Scrap",
            "ISSUE_COUNTS": config.count,
            "ERROR_MESSAGE": error,
            "STATUS": status,
            "EXECUTION_TIME": str(execution_time),
            "CREATED": config.created
        }
        self.insert_log(log_data)
        config.main_db.close_connection()
        self.close_connection()