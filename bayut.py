import os
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

def main(file_type, purpose, category, search, page_num):
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

    db.insert_status()

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
    json_data2 = ""
    try:
        with open(filename, 'rb') as mhtml_file:
            # Parse the mhtml content
            msg = email.message_from_bytes(mhtml_file.read())
        # Find the HTML part
        html_content = ""
        for part in msg.walk():
            if part.get_content_type() == "text/html":
                html_content = part.get_payload(decode=True).decode(part.get_content_charset('utf-8'))
                break
        tree = html.fromstring(html_content)
        data = {}
        area_text = tree.xpath('//span[@aria-label="Area"]//text()')
        for text in area_text:
            if len(text.strip()) > 0:
                area_text = text
                break
        data['size'] = area_text
        description_text = tree.xpath('//div[@aria-label="Property description"]//text()')
        description_text = ' '.join(text.strip() for text in description_text if text.strip())
        data['description'] = description_text
        ul_tags_with_columns = tree.xpath('//ul[contains(@style, "columns:2")]')

        for ids, ul_tag in enumerate(ul_tags_with_columns):
            for li_tag in ul_tag.xpath('//li'):
                spans = li_tag.findall('span')
                if len(spans) >= 2:
                    key = spans[0].text.strip()
                    value = spans[1].xpath('./text()')  # Get text from the second span directly
                    if not value:
                        value = spans[1].xpath('.//text()')
                    # Clean up whitespace
                    value = ' '.join(text.strip() for text in value if text.strip())
                    data[key] = value
        json_data2 = json.dumps(data, indent=4)
    except FileNotFoundError:
        print(f"File not found: {filename}")
    return json_data2

def download_webpage(externalID):

    url = f"https://www.bayut.com/property/details-{externalID}.html"

    while True:
        filename = f'webpage{externalID}.mhtml'
        if os.path.exists('temp/'+ filename):
            pyautogui.hotkey('ctrl', 'w')
            time.sleep(1)
            break
        webbrowser.open(url)

        pyautogui.hotkey('win', 'up')

        time.sleep(3)

        pyautogui.hotkey('ctrl', 's')

        time.sleep(1)

        pyautogui.typewrite(filename)

        time.sleep(1)

        pyautogui.press('enter')

        time.sleep(1)

        if os.path.exists('temp/'+ filename):
            pyautogui.hotkey('ctrl', 'w')
            time.sleep(1)
            break

if __name__ == '__main__':
    purposes = ["for-sale", "for-rent"]
    categories = ["residential", "commercial"]
    result_file_type = sys.argv[1]
    purpose = sys.argv[2]
    category = sys.argv[3]
    search = None
    page_num = 0
    if len(sys.argv) > 4:
        search = sys.argv[4]
    if len(sys.argv) > 5:
        page_num = int(sys.argv[5])

    if not purpose in purposes:
        print(f"Purpose value is wrong: {sys.argv[1]}")
        exit(1)
    if not category in categories:
        print(f"category value is wrong: {sys.argv[2]}")
        exit(1)
    remove_all_files_in_folder('temp')
    main(result_file_type, purpose, category, search, page_num)

