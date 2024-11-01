import sys
import time
import requests
import config
import json
import csv
import webbrowser
import pyautogui
from lxml import html
from database import MySQLDatabase
import email
from pathlib import Path
from utils import get_extracted_data, get_raw_data, get_params, get_headers, get_request_url

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
            download_webpage(hit.get("externalID"))
        for hit in hints:
            property = get_detail_information(hit.get("externalID"))
            item = get_extracted_data(hit, property)
            if file_type != "csv":
                db.check_item_and_update_or_insert(item)
            else:
                save_csv_file(count, item)
            count += 1
        remove_all_files_in_folder('temp')
        time.sleep(5)
        if len(hints) == 0:
            print(f"Total of items is {count}.")
            break
        page_num += 1
        break


def remove_all_files_in_folder(folder_path):
    path = Path(folder_path)
    if path.exists() and path.is_dir():
        for file in path.iterdir():
            if file.is_file():  # Check if it is a file
                file.unlink()
def save_csv_file(count, item):
    csv_file = 'result/bayut.csv'
    with open(csv_file, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        if count == 0:
            writer.writerow(item.keys())
        writer.writerow(item.values())
        print(f"Inserting new item with id: {item['id']}")

def get_detail_information(externalID):
    filename = f"temp/webpage{externalID}.mhtml"
    with open(filename, 'rb') as mhtml_file:
        # Parse the mhtml content
        msg = email.message_from_bytes(mhtml_file.read())

    # Find the HTML part
    html_content = ""
    for part in msg.walk():
        # Look for HTML content
        if part.get_content_type() == "text/html":
            html_content = part.get_payload(decode=True).decode(part.get_content_charset('utf-8'))
            break
    tree = html.fromstring(html_content)
    size = tree.xpath('//*[@id="body-wrapper"]/main/div[2]/div[4]/div[1]/div[3]/div[1]/span[2]/span[1]/span[1]/text()')[0]
    description = tree.xpath('//*[@id="body-wrapper"]/main/div[2]/div[4]/div[3]/div[1]/div/div[1]/div[1]/div/div/div/span/text()')[0]
    property_ul = tree.xpath('//*[@id="body-wrapper"]/main/div[2]/div[4]/div[3]/div[1]/div/div[2]/ul')
    building_ul = tree.xpath('//*[@id="body-wrapper"]/main/div[2]/div[4]/div[3]/div[4]/ul')
    validation_ul = tree.xpath('//*[@id="body-wrapper"]/main/div[2]/div[4]/div[3]/div[2]/ul')
    data = {}
    data['size'] = size
    data['description'] = description
    if property_ul:
        for li in property_ul[0].findall('li'):
            spans = li.findall('span')
            if len(spans) >= 2:  # Ensure there are at least two <span> elements
                key = spans[0].text.strip()
                # Check if the second span contains a nested div
                if spans[1].find('div'):
                    value = spans[1].find('div').text.strip()
                else:
                    value = spans[1].text.strip()
                data[key] = value
    if building_ul:
        for li in building_ul[0].xpath("./li"):
            spans = li.findall('span')
            if len(spans) >= 2:
                key = spans[0].text.strip()
                value = ""
                if spans[1].xpath('./div/div/div/span/span'):
                    value = spans[1].xpath('./div/div/div/span/span')[0].text.strip()
                elif spans[1].xpath('./div/div[1]/div'):
                    value = spans[1].xpath('./div/div[1]/div')[0].text.strip()
                elif spans[1].xpath('./div/div/div'):
                    value = spans[1].xpath('./div/div/div')[0].text.strip()
                else:
                    value = spans[1].text.strip()
                data[key] = value
    if validation_ul:
        for li in validation_ul[0].xpath("./li"):
            spans = li.findall('span')
            if len(spans) >= 2:
                key = spans[0].text.strip()
                value = ""
                if spans[1].xpath('./div/div/div/span/span'):
                    value = spans[1].xpath('./div/div/div/span/span')[0].text.strip()
                elif spans[1].xpath('./div/div[1]/div'):
                    value = spans[1].xpath('./div/div[1]/div')[0].text.strip()
                elif spans[1].xpath('./div/div/div'):
                    value = spans[1].xpath('./div/div/div')[0].text.strip()
                else:
                    value = spans[1].text.strip()
                data[key] = value
    json_data2 = json.dumps(data, indent=4)
    return json_data2

def download_webpage(externalID):

    url = f"https://www.bayut.com/property/details-{externalID}.html"

    webbrowser.open(url)

    time.sleep(3)

    for _ in range(30):  # Adjust the range as needed
        pyautogui.press("down")  # Press the Down Arrow key to scroll down
        time.sleep(0.2)

    pyautogui.hotkey('ctrl', 's')

    time.sleep(1)

    pyautogui.typewrite(f'webpage{externalID}.mhtml')

    pyautogui.press('enter')

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

