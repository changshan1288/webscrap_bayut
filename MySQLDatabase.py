import pymysql
import config
class MySQLDatabase:
    def __init__(self, host, user, password, database, port=3306):
        """Initialize the MySQLDatabase class with connection parameters."""
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.connection = None

    def open_connection(self):
        """Open a connection to the MySQL database."""
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

    def close_connection(self):
        """Close the MySQL database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
            print("MySQL connection closed.")

    def execute_query(self, query, params=None):
        """Execute a query on the MySQL database."""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                self.connection.commit()
                print("Query executed successfully.")
        except pymysql.MySQLError as e:
            print(f"Failed to execute query: {e}")

    def fetch_all(self, query, params=None):
        """Execute a query and fetch all results."""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                result = cursor.fetchall()
                return result
        except pymysql.MySQLError as e:
            print(f"Failed to fetch data: {e}")
            return None
    def check_item_and_update_or_insert(self, item):
        query = (f"SELECT updatedAt FROM {config.TABLE_NAME} WHERE unique_id = %s")
        result = self.fetch_all(query, item["id"])
        if result:
            date1_obj = item["updatedAt"]
            date2_obj = result[0][0]

            if date1_obj == date2_obj:
                print(f"Duplicated id: {item['id']}")
            else:
                print(f"Updated id: {item['id']}")
                self.update_item(item)
        else:
            print(f"Inserting new item with id: {item['id']}")
            self.insert_item(item)

    def update_item(self, item):
        query = (
            f"UPDATE {config.TABLE_NAME} SET ownerID=%s, title=%s, baths=%s, rooms=%s, price=%s, createdAt=%s, updatedAt=%s, reactivatedAt=%s, area=%s, plotArea=%s, location=%s, category=%s, mobile=%s, phone=%s, whatsapp=%s, proxyPhone=%s, contactName=%s, permitNumber, ded=%s, rera=%s, orn=%s, description=%s, type=%s, purpose=%s, reference_no=%s, completion=%s, furnishing=%s, truCheck=%s, added_on=%s, handover_date=%s, ownerAgent=%s, agency=%s WHERE unique_id=%s")
        params = (item["ownerID"],
                  item["title"], item["baths"], item["rooms"], item["price"],
                  item["createdAt"], item["updatedAt"], item["reactivatedAt"],
                  item["area"], item["plotArea"], item["location"], item["category"],
                  item["mobile"], item["phone"], item["whatsapp"], item["proxyPhone"],
                  item["contactName"], item["permitNumber"], item["ded"], item["rera"],
                  item["orn"], item["description"], item["type"], item["purpose"], item["reference_no"],
                  item["completion"], item["furnishing"], item["truCheck"],
                  item["added_on"], item["handover_date"],
                  item["ownerAgent"], item["agency"], item["id"])
        self.execute_query(query, params)

    def init_table(self):
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {config.TABLE_NAME} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                unique_id INT,
                ownerID INT,
                title VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
                baths INT,
                rooms INT,
                price FLOAT,
                createdAt DATETIME,
                updatedAt DATETIME,
                reactivatedAt DATETIME,
                area FLOAT,
                plotArea FLOAT,
                location VARCHAR(255),
                category VARCHAR(255),
                mobile VARCHAR(20),
                phone VARCHAR(20),
                whatsapp VARCHAR(20),
                proxyPhone VARCHAR(20),
                contactName VARCHAR(255),
                permitNumber VARCHAR(100),
                ded VARCHAR(50),
                rera VARCHAR(50),
                orn VARCHAR(50),
                description LONGTEXT,
                type VARCHAR(50),
                purpose VARCHAR(50),
                reference_no VARCHAR(50),
                completion VARCHAR(50),
                furnishing VARCHAR(50),
                truCheck VARCHAR(50),
                added_on VARCHAR(50),
                handover_date VARCHAR(50),
                ownerAgent VARCHAR(255),
                agency VARCHAR(255),
                property_link VARCHAR(255)
            );
        """
        self.execute_query(create_table_sql)
    def insert_item(self, item):
        query = (f"INSERT INTO {config.TABLE_NAME}(unique_id, ownerID, title, baths, rooms, price, createdAt, updatedAt, reactivatedAt, area, plotArea, location, category, mobile, phone, whatsapp, proxyPhone, contactName, permitNumber, ded, rera, orn, description, type, purpose, reference_no, completion, furnishing, truCheck, added_on, handover_date, ownerAgent, agency, property_link ) VALUES "
                 f"(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
        params = ( item["id"], item["ownerID"],
            item["title"], item["baths"], item["rooms"], item["price"],
            item["createdAt"], item["updatedAt"], item["reactivatedAt"],
            item["area"], item["plotArea"], item["location"], item["category"],
            item["mobile"], item["phone"], item["whatsapp"], item["proxyPhone"],
            item["contactName"], item["permitNumber"], item["ded"], item["rera"],
            item["orn"], item["description"], item["type"], item["purpose"], item["reference_no"],
            item["completion"], item["furnishing"], item["truCheck"],
            item["added_on"], item["handover_date"],
            item["ownerAgent"], item["agency"], item["property_link"]
        )
        self.execute_query(query, params)