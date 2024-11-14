from datetime import datetime
import mysql.connector
from mysql.connector import Error
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
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                port=self.port,
                connection_timeout=60,  # Connection timeout in seconds
                use_pure=True
            )

            if self.connection.is_connected():
                self.cursor = self.connection.cursor()
                print("Successfully connected to MySQL database")
        except Error as e:
            print("Error while connecting to MySQL", e)

    def close_connection(self):
        """Close the cursor and the database connection."""
        if self.cursor:
            self.cursor.close()
        if self.connection and self.connection.is_connected():
            self.connection.close()
        print("MySQL connection is closed")

    def execute_query(self, query, params=None):
        if self.connection is None or not self.connection.is_connected():
            print("Connection is not established. Please connect first.")
            return
        try:
            self.cursor.execute(query, params)
            self.connection.commit()  # Committing the transaction
            print(f"{self.cursor.rowcount} rows inserted successfully.")
        except Error as e:
            config.status_db.insert_status_log("ERROR", f"Error while inserting data into MySQL: {e}")

    def fetch_all(self, query, params=None):

        if self.connection is None or not self.connection.is_connected():
            print("Connection is not established. Please connect first.")
            return

        try:
            # Execute the query to fetch all rows
            self.cursor.execute(query, params)
            # Fetch all rows and return them
            records = self.cursor.fetchall()
            return records

        except Error as e:
            config.status_db.insert_status_log("ERROR", f"Error while fetching data from MySQL: {e}")
            return None

    def check_item_and_update_or_insert(self, item):
        query = (f"SELECT updatedAt FROM {config.TABLE_NAME} WHERE id = %s")
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
        self.insert_item_for_trend(item)

    def update_item(self, json_data):
        update_query = f"""
                    UPDATE {config.TABLE_NAME}
                    SET
                        ownerID = %s, title = %s, baths = %s,
                        rooms = %s, price = %s, createdAt = %s,
                        updatedAt = %s, reactivatedAt = %s, area = %s,
                        plotArea = %s, location = %s, category = %s,
                        mobile = %s, phone = %s, whatsapp = %s,
                        proxyPhone = %s, contactName = %s,
                        permitNumber = %s, ded = %s, rera = %s,
                        orn = %s, type_value = %s, purpose = %s,
                        reference_no = %s, completion = %s,
                        furnishing = %s, truCheck = %s,
                        added_on = %s, handover_date = %s,
                        description = %s, size_value = %s,
                        building_name = %s, park_spaces = %s,
                        floors = %s, building_area = %s,
                        swimming_pools = %s, elevators = %s,
                        offices = %s, shops = %s, developers = %s,
                        built_up_Area = %s, usage_value = %s,
                        parking_availability = %s, ownership = %s,
                        ownerAgent = %s, agency = %s, retail_centres = %s,
                        property_link = %s, is_available = %s
                    WHERE id = %s
                    """

        values = (
            json_data['ownerID'], json_data['title'],
            json_data['baths'], json_data['rooms'],
            json_data['price'], json_data['createdAt'],
            json_data['updatedAt'], json_data['reactivatedAt'],
            json_data['area'], json_data['plotArea'],
            json_data['location'], json_data['category'],
            json_data['mobile'], json_data['phone'],
            json_data['whatsapp'], json_data['proxyPhone'],
            json_data['contactName'], json_data['permitNumber'],
            json_data['ded'], json_data['rera'],
            json_data['orn'], json_data['type'],
            json_data['purpose'], json_data['reference_no'],
            json_data['completion'], json_data['furnishing'],
            json_data['truCheck'], json_data['added_on'],
            json_data['handover_date'], json_data['description'],
            json_data['size'], json_data['building_name'],
            json_data['park_spaces'], json_data['floors'],
            json_data['building_area'], json_data['swimming_pools'],
            json_data['elevators'], json_data['offices'],
            json_data['shops'], json_data['developers'],
            json_data['built_up_Area'], json_data['usage'],
            json_data['parking_availability'], json_data['ownership'],
            json_data['ownerAgent'], json_data['agency'],
            json_data['retail_centres'],
            json_data['property_link'],
            json_data['is_available'],
            json_data['id']  # This is the condition to match the record to update
        )
        self.execute_query(update_query, values)

    def init_table(self):
        if self.connection is None or not self.connection.is_connected():
            print("Connection is not established. Please connect first.")
            return
        create_table_sql = f"""
                    CREATE TABLE IF NOT EXISTS {config.TABLE_NAME} (
                        id INT AUTO_INCREMENT PRIMARY KEY,
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
                        description TEXT,
                        type_value VARCHAR(50),
                        purpose VARCHAR(50),
                        reference_no VARCHAR(50),
                        completion VARCHAR(50),
                        furnishing VARCHAR(50),
                        truCheck VARCHAR(50),
                        added_on VARCHAR(50),
                        handover_date VARCHAR(50),
                        ownerAgent VARCHAR(255),
                        agency VARCHAR(255),
                        property_link VARCHAR(255),
                        size_value VARCHAR(255),
                        building_name VARCHAR(255),
                        park_spaces INT,
                        floors INT,
                        building_area VARCHAR(255),
                        swimming_pools INT,
                        elevators INT,
                        offices INT,
                        shops INT,
                        developers VARCHAR(255),
                        built_up_Area VARCHAR(255),
                        usage_value VARCHAR(255),
                        parking_availability VARCHAR(255),
                        retail_centres VARCHAR(255),
                        ownership VARCHAR(255),
                        is_available TINYINT(1)
                    );
                """
        self.execute_query(create_table_sql)

        create_table_sql = f"""
                    CREATE TABLE IF NOT EXISTS {config.TRAND_TABLE_NAME} (
                        id INT AUTO_INCREMENT PRIMARY KEY,
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
                        description TEXT,
                        type_value VARCHAR(50),
                        purpose VARCHAR(50),
                        reference_no VARCHAR(50),
                        completion VARCHAR(50),
                        furnishing VARCHAR(50),
                        truCheck VARCHAR(50),
                        added_on VARCHAR(50),
                        handover_date VARCHAR(50),
                        ownerAgent VARCHAR(255),
                        agency VARCHAR(255),
                        property_link VARCHAR(255),
                        size_value VARCHAR(255),
                        building_name VARCHAR(255),
                        park_spaces INT,
                        floors INT,
                        building_area VARCHAR(255),
                        swimming_pools INT,
                        elevators INT,
                        offices INT,
                        shops INT,
                        developers VARCHAR(255),
                        built_up_Area VARCHAR(255),
                        usage_value VARCHAR(255),
                        parking_availability VARCHAR(255),
                        retail_centres VARCHAR(255),
                        ownership VARCHAR(255),
                        is_available TINYINT(1),
                        scrap_date DATETIME
                    );
                """
        self.execute_query(create_table_sql)

    def insert_item(self, item):
        insert_query = f"""
                INSERT INTO {config.TABLE_NAME} (
                    id, ownerID, title, baths, rooms, price, createdAt, updatedAt,
                    reactivatedAt, area, plotArea, location, category, mobile, phone,
                    whatsapp, proxyPhone, contactName, permitNumber, ded, rera, orn,
                    type_value, purpose, reference_no, completion, furnishing, truCheck, added_on,
                    handover_date, description, size_value, building_name, park_spaces, floors,
                    building_area, swimming_pools, elevators, offices, shops, developers,
                    built_up_Area, usage_value, parking_availability, retail_centres, ownership, ownerAgent,
                    agency, property_link, is_available
                ) VALUES (
                    %(id)s, %(ownerID)s, %(title)s, %(baths)s, %(rooms)s, %(price)s, %(createdAt)s,
                    %(updatedAt)s, %(reactivatedAt)s, %(area)s, %(plotArea)s, %(location)s,
                    %(category)s, %(mobile)s, %(phone)s, %(whatsapp)s, %(proxyPhone)s, 
                    %(contactName)s, %(permitNumber)s, %(ded)s, %(rera)s, %(orn)s, %(type)s,
                    %(purpose)s, %(reference_no)s, %(completion)s, %(furnishing)s, 
                    %(truCheck)s, %(added_on)s, %(handover_date)s, %(description)s, 
                    %(size)s, %(building_name)s, %(park_spaces)s, %(floors)s, 
                    %(building_area)s, %(swimming_pools)s, %(elevators)s, 
                    %(offices)s, %(shops)s, %(developers)s, %(built_up_Area)s, 
                    %(usage)s, %(parking_availability)s,  %(retail_centres)s, %(ownership)s,
                    %(ownerAgent)s, %(agency)s, %(property_link)s, %(is_available)s
                )
                """
        self.execute_query(insert_query, item)

    def insert_item_for_trend(self, item):
        item["created"] = config.created
        insert_query = f"""
            INSERT INTO {config.TRAND_TABLE_NAME} (
                ownerID, title, baths, rooms, price, createdAt, updatedAt,
                reactivatedAt, area, plotArea, location, category, mobile, phone,
                whatsapp, proxyPhone, contactName, permitNumber, ded, rera, orn,
                type_value, purpose, reference_no, completion, furnishing, truCheck, added_on,
                handover_date, description, size_value, building_name, park_spaces, floors,
                building_area, swimming_pools, elevators, offices, shops, developers,
                built_up_Area, usage_value, parking_availability, retail_centres, ownership, ownerAgent,
                agency, property_link, is_available, scrap_date
            ) VALUES (
                %(ownerID)s, %(title)s, %(baths)s, %(rooms)s, %(price)s, %(createdAt)s,
                %(updatedAt)s, %(reactivatedAt)s, %(area)s, %(plotArea)s, %(location)s,
                %(category)s, %(mobile)s, %(phone)s, %(whatsapp)s, %(proxyPhone)s, 
                %(contactName)s, %(permitNumber)s, %(ded)s, %(rera)s, %(orn)s, %(type)s,
                %(purpose)s, %(reference_no)s, %(completion)s, %(furnishing)s, 
                %(truCheck)s, %(added_on)s, %(handover_date)s, %(description)s, 
                %(size)s, %(building_name)s, %(park_spaces)s, %(floors)s, 
                %(building_area)s, %(swimming_pools)s, %(elevators)s, 
                %(offices)s, %(shops)s, %(developers)s, %(built_up_Area)s, 
                %(usage)s, %(parking_availability)s,  %(retail_centres)s, %(ownership)s,
                %(ownerAgent)s, %(agency)s, %(property_link)s, %(is_available)s, %(created)s
            )
            """
        self.execute_query(insert_query, item)