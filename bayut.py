import datetime
import sys
import time
import requests
import config
import json
import csv
from MySQLDatabase import MySQLDatabase
from utils import get_extracted_data, get_raw_data, get_params, get_headers, get_request_url
from bs4 import BeautifulSoup

def main(file_type, purpose, category, search):
    db = MySQLDatabase(
        host=config.MYSQL_HOST,
        user=config.MYSQL_USER,
        password=config.MYSQL_PASSWORD,
        database=config.MYSQL_DB
    )

    db.open_connection()

    db.init_table()

    url = get_request_url()
    params = get_params()

    headers = get_headers()

    page_num = 0
    count = 0
    while True:
        print(f"page_num: {page_num+1}")

        raw_data = get_raw_data(page_num, purpose, category, search)

        response = requests.post(url, params=params, json=raw_data, headers=headers)

        json_data = json.loads(response.text)
        results = json_data.get("results")
        hints = results[0].get("hits")
        for hit in hints:
            property = get_detail_information(hit.get("externalID"))
            item = get_extracted_data(hit, property)
            if file_type != "csv":
                db.check_item_and_update_or_insert(item)
            else:
                save_csv_file(count, item)
            count += 1
        time.sleep(5)
        if len(hints) == 0:
            print(f"Total of items is {count}.")
            break
        page_num += 1

def save_csv_file(count, item):
    csv_file = 'result/bayut.csv'
    with open(csv_file, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        if count == 0:
            writer.writerow(item.keys())
        writer.writerow(item.values())
        print(f"Inserting new item with id: {item['id']}")

def get_detail_information(externalID):
    url = f"https://www.bayut.com/property/details-{externalID}.html"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36'
    }
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    main_tag = soup.find('main')
    property_information = main_tag.find('ul', {"aria-label": "Property details"})
    json_data1 = ""
    data = {}
    if property_information:
        for li in property_information.find_all('li'):
            spans = li.find_all('span')
            if len(spans) >= 2:  # Ensure there are at least two <span> elements
                key = spans[0].text.strip()
                # Check if the second span contains a nested div
                if spans[1].find('div'):
                    value = spans[1].find('div').text.strip()
                else:
                    value = spans[1].text.strip()
                data[key] = value
        json_data1 = json.dumps(data, indent=4)
    return json_data1

if __name__ == '__main__':
    purposes = ["for-sale", "for-rent"]
    categories = ["residential", "commercial"]
    result_file_type = sys.argv[1]
    purpose = sys.argv[2]
    category = sys.argv[3]
    search = None
    if len(sys.argv) > 4:
        search = sys.argv[4]
    if not purpose in purposes:
        print(f"Purpose value is wrong: {sys.argv[1]}")
        exit(1)
    if not category in categories:
        print(f"category value is wrong: {sys.argv[2]}")
        exit(1)
    main(result_file_type, purpose, category, search)

