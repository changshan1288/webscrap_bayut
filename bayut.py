import os
import sys
import time
import requests
import config
import json
import webbrowser
import pyautogui
from lxml import html
from database import MySQLDatabase
import email
from pathlib import Path
from datetime import datetime

from database_others import MySQLDatabaseOther
from utils import get_extracted_data, get_raw_data, get_params, get_headers, get_request_url

def main(purpose, category, search, page_num):

    url = get_request_url()
    params = get_params()

    headers = get_headers()

    config.count = 0

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
            extracted_data = get_detail_information(hit.get("externalID"))
            item = get_extracted_data(hit, extracted_data)
            config.main_db.check_item_and_update_or_insert(item)
            config.count += 1
        remove_all_files_in_folder(config.UTILS_DIR + '/temp')
        time.sleep(3)
        if len(hints) == 0:
            config.status_db.insert_status_log("SUCCESS")
            break
        page_num += 1



def remove_all_files_in_folder(folder_path):
    path = Path(folder_path)
    if path.exists() and path.is_dir():
        for file in path.iterdir():
            if file.is_file():  # Check if it is a file
                file.unlink()


def get_detail_information(externalID):
    filename = f"{config.UTILS_DIR}/temp/webpage{externalID}.mhtml"
    json_data = ""
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

        span_available = tree.xpath('//span[@aria-label="Inactive property banner"]//text()')
        is_available = 1
        if span_available:
            is_available = 0
        data['is_available'] = is_available

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
        json_data = json.dumps(data, indent=4)
    except FileNotFoundError:
        config.status_db.insert_status_log("ERROR", f"File not found: {filename}")
    return json_data

def download_webpage(externalID):

    url = f"https://www.bayut.com/property/details-{externalID}.html"
    refind = 0
    while True:
        filename = f'webpage{externalID}.mhtml'
        try:
            if os.path.exists(config.UTILS_DIR + '/temp/'+ filename):
                for _ in range(refind):
                    pyautogui.hotkey('ctrl', 'w')
                    time.sleep(1)
                break
            else:
                refind += 1
            webbrowser.open(url)

            time.sleep(3)

            pyautogui.hotkey('win', 'up')

            time.sleep(1)

            pyautogui.hotkey('ctrl', 's')

            time.sleep(1)

            pyautogui.typewrite(filename)

            time.sleep(1)

            pyautogui.press('enter')

            time.sleep(1)

            if os.path.exists(config.UTILS_DIR + '/temp/'+ filename):
                for _ in range(refind):
                    pyautogui.hotkey('ctrl', 'w')
                    time.sleep(1)
                break
            else:
                refind += 1
        except pyautogui.FailSafeException:
            config.status_db.insert_status_log("ERROR", "Fail-safe triggered! Mouse moved to the corner.")
        except pyautogui.ImageNotFoundException:
            config.status_db.insert_status_log("ERROR", "Image not found on the screen.")
        except Exception as e:
            config.status_db.insert_status_log("ERROR", f"An unexpected error occurred: {e}")

if __name__ == '__main__':
    purposes = ["for-sale", "for-rent"]
    categories = ["residential", "commercial"]
    purpose = sys.argv[1]
    category = sys.argv[2]
    search = None
    page_num = 0
    if len(sys.argv) > 3:
        search = sys.argv[3]
    if len(sys.argv) > 4:
        page_num = int(sys.argv[4])

    remove_all_files_in_folder(config.UTILS_DIR + '/temp')
    config.created = datetime.now()
    print(f"scrap start : {config.created}")

    config.main_db = MySQLDatabase(
        host=config.MYSQL_HOST,
        user=config.MYSQL_USER,
        password=config.MYSQL_PASSWORD,
        database=config.MYSQL_DB
    )

    config.status_db = MySQLDatabaseOther(
        host=config.MYSQL_HOST,
        user=config.MYSQL_USER,
        password=config.MYSQL_PASSWORD,
        database=config.MYSQL_DB_OTHERS
    )

    config.main_db.open_connection()
    config.status_db.open_connection()

    config.main_db.init_table()
    config.status_db.init_table()

    main(purpose, category, search, page_num)

