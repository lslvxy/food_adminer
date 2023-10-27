import json
import logging
import time
import os
import re
import urllib

import requests
import pandas as pd
from datetime import datetime

from utils import isEn, init_path, fixStr, fix_price, parse
from utils import isCn
from utils import isTh
import pathlib
from sqlite3 import OperationalError
import sqlite3
import shutil
from pathlib import Path

bc = logging.basicConfig(level=logging.INFO, format='%(asctime)s  - %(message)s')


class FoodPandaItem:
    biz_type = 'food_panda'
    batchNo = ''
    id = ''
    language = ''
    chain_code = ''
    chain_id = ''
    chain_name = ''
    store_id = ''
    store_code = ''
    store_name = ''
    store_url = ''
    store_address = ''
    longitude = ''
    latitude = ''
    country = ''
    city = ''
    location = ''
    photoHref = ''
    image_list = []
    categories = []
    menus = []
    toppings = []
    total_category_list = []
    total_product_map = {}
    total_group_map = {}
    total_modifier_map = {}
    fina_data = []
    tree = False
    total_sub_group = []
    total_sub_item = []
    chain = {}
    store_data = {}


class Config:
    db_path: ''
    batchNo = ''


def parse_foodpandaV2(variables):
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    batch_no = f'{timestamp}'
    print(f'run batch no:{batch_no}')
    logging.info(f'run batch no:{batch_no}')
    url_list = variables['url_list']
    global_config = Config()
    global_config.batchNo = batch_no
    prepare_data(global_config)
    conn = sqlite3.connect(global_config.db_path)
    for p_idx, page_url in enumerate(url_list):
        variables['run_index'] = p_idx
        variables['total_count'] = len(url_list)
        print(f'processing: {p_idx + 1}/{len(url_list)}')
        root_data = fetch_data(page_url, variables)
        store_data = root_data.get('data', None)

        if store_data is None:
            continue
        item = FoodPandaItem()
        item.run_index = variables['run_index']
        item.total_count = variables['total_count']
        item.batchNo = batch_no
        item.store_data = store_data
        item.store_id = store_data.get('id')
        item.store_name = store_data.get('name')
        item.store_code = store_data.get('code')
        item.store_url = store_data.get('web_path')
        item.store_address = store_data.get('address')

        item.chain_id = store_data.get('chain')['id']
        item.chain_name = store_data.get('chain')['name']
        item.chain_code = store_data.get('chain')['code']
        item.longitude = store_data.get('longitude')
        item.latitude = store_data.get('latitude')
        item.country = store_data.get('chain')
        item.city = store_data.get('city')['name']
        item.location = store_data.get('location')
        item.customer_phone = store_data.get('customer_phone')

        item.photoHref = store_data.get('photoHref')
        item.chain = store_data.get('chain')

        item.language = variables['language']
        item.country = variables['country']

        process_store(item)
        save_to_db(item, conn)
        time.sleep(int(variables['interval']))
        del item
        del root_data
    # clean_data(item)
    # compose_images(item)
    # if not '?img=no' in page_url:
    #     save_images(item)
    # process_category(item)
    # process_product(item)
    # process_group(item)
    # process_item(item)
    # if item.tree:
    # process_sub_group(item)
    # process_sub_item(item)

    # process_final_list(item, conn)
    process_excel(global_config, conn)

    return True


def clean_data(item):
    homedir = str(pathlib.Path.home())
    store_path = os.path.join(homedir, "Aim_menu", item.biz_type, f"{fixStr(item.store_name.strip())}")
    if os.path.exists(store_path):
        shutil.rmtree(store_path)
    print("remove store dir")


def prepare_data(item):
    homedir = str(pathlib.Path.home())
    dir_path = os.path.join(homedir, "Aim_menu", "store")
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    db_path = os.path.join(homedir, "Aim_menu", ".data.db")
    item.db_path = db_path
    if not os.path.exists(db_path):
        Path(db_path).touch()
        print("db init success！")
    conn = sqlite3.connect(db_path)
    conn.set_trace_callback(print)

    cur = conn.cursor()
    try:
        sql = """CREATE TABLE "store_list" (
                  "batch_no" varchar(64) NOT NULL,
                  "biz_type" varchar(64) NOT NULL,
                  "web_path" varchar(2048),
                  "ID" varchar(255),
                  "code" varchar(255),
                  "accepts_instructions" varchar(255),
                  "address" varchar(2048),
                  "address_line2" varchar(2048),
                  "budget" varchar(255),
                  "chain_id" varchar(255),
                  "chain_main_vendor_id" varchar(255),
                  "chain_name" varchar(255),
                  "chain_code" varchar(255),
                  "city_id" varchar(255),
                  "city_name" varchar(255),
                  "cuisines_id" varchar(255),
                  "cuisines_name" varchar(2048),
                  "cuisines_main" varchar(255),
                  "primary_cuisine_id" varchar(255),
                  "primary_cuisine_name" varchar(255),
                  "primary_cuisine_main" varchar(255),
                  "food_characteristics_id" varchar(255),
                  "food_characteristics_name" varchar(255),
                  "food_characteristics_is_halal" varchar(255),
                  "food_characteristics_is_vegetarian" varchar(255),
                  "customer_phone" varchar(255),
                  "hero_image" varchar(2048),
                  "hero_listing_image" varchar(2048),
                  "description" varchar(2048),
                  "is_active" varchar(255),
                  "is_checkout_comment_enabled" varchar(255),
                  "is_new_until" varchar(255),
                  "is_delivery_enabled" varchar(255),
                  "is_pickup_enabled" varchar(255),
                  "is_preorder_enabled" varchar(255),
                  "is_service_fee_enabled" varchar(255),
                  "is_service_tax_enabled" varchar(255),
                  "is_service_tax_visible" varchar(255),
                  "is_vat_disabled" varchar(255),
                  "is_vat_included_in_product_price" varchar(255),
                  "is_vat_visible" varchar(255),
                  "is_vat_included" varchar(255),
                  "latitude" varchar(255),
                  "location" varchar(255),
                  "logo" varchar(2048),
                  "longitude" varchar(255),
                  "food_license_number" varchar(255),
                  "timezone" varchar(255),
                  "minimum_order_amount" varchar(255),
                  "minimum_pickup_time" varchar(255),
                  "multiple_toppings" varchar(255),
                  "name" varchar(2048),
                  "post_code" varchar(255),
                  "rating" varchar(255),
                  "review_number" varchar(255),
                  "weekday_1_opening" varchar(255),
                  "weekday_1_closing" varchar(255),
                  "weekday_2_opening" varchar(255),
                  "weekday_2_closing" varchar(255),
                  "weekday_3_opening" varchar(255),
                  "weekday_3_closing" varchar(255),
                  "weekday_4_opening" varchar(255),
                  "weekday_4_closing" varchar(255),
                  "weekday_5_opening" varchar(255),
                  "weekday_5_closing" varchar(255),
                  "weekday_6_opening" varchar(255),
                  "weekday_6_closing" varchar(255),
                  "weekday_7_opening" varchar(255),
                  "weekday_7_closing" varchar(255),
                  "service_fee" varchar(255),
                  "service_fee_percentage_amount" varchar(255),
                  "service_tax_percentage_amount" varchar(255),
                  "short_name" varchar(2048),
                  "small_order_fee" varchar(255),
                  "vat_percentage_amount" varchar(255),
                  "tag" varchar(255),
                  "tags" varchar(255),
                  "trade_register_number" varchar(255),
                  "legal_name" varchar(2048),
                  "address_line_1" varchar(2048)
                );"""

        cur.execute(sql)
        print("create table success")
        return True
    except OperationalError as o:
        # print(str(o))
        pass
        if str(o) == "table product_list already exists":
            return True
        return False
    except Exception as e:
        # print(e)
        return False
    finally:
        cur.close()
        conn.close()

    pass


def fetch_data(page_url, variables):
    variables = parse(page_url)
    api_url = 'https://%s.fd-api.com/api/v5/vendors/%s?include=bundles,multiple_discounts&language_id=6&basket_currency=TWD&show_pro_deals=true'
    complete_url = api_url % (variables['country'], variables['id'])
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36 Edg/116.0.1938.69',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Encoding': 'gzip, deflate, br'
    }
    payload = {}
    response = requests.request("GET", complete_url, headers=headers, data=payload)
    i = 0
    while i < 10:
        if response.status_code == 200:
            # print("Request to page, data being pulled")
            break
        i += 1
        print("Page response failed, retrying")
        time.sleep(5)
        response = requests.request("GET", complete_url, headers=headers, data=payload)
    if i == 10 and response.status_code != 200:
        print("Page response failed, please check network link and try again later")
        return None
    return response.json()


def compose_images(item):
    image_list = []
    store_name = fixStr(item.store_name)
    all_dir_path = init_path('food_panda', store_name, "ALL")
    for ca in item.menus:
        category_name = f"{fixStr(ca['name'])}"
        category_dir_path = init_path('food_panda', store_name, category_name)
        product_list = ca['products']
        for pd in product_list:
            if pd['file_path']:
                file_name = str(pd['id'])
                image_list.append({
                    'category': category_name,
                    'all_dir_path': all_dir_path,
                    'category_dir_path': category_dir_path,
                    'url': pd['file_path'],
                    'file_name': file_name + '.jpg'
                })
    item.image_list = image_list


def save_images(item):
    for img in item.image_list:
        if not img['url']:
            continue
        file_name = img['file_name']
        image_path = os.path.join(img['category_dir_path'], file_name)
        image_path2 = os.path.join(img['all_dir_path'], file_name)
        retry = 3
        for r in range(retry):
            try:
                r = requests.get(img['url'], timeout=180)
                blob = r.content
                with open(image_path, 'wb') as f:
                    f.write(blob)
                with open(image_path2, 'wb') as f:
                    f.write(blob)
            except Exception as e:
                if r < 2:
                    logging.error(f'Failed. Attempt # {r + 1}')
                else:
                    print('Error downloading image at third attempt')
            else:
                print("Download image: " + image_path)
                break


def process_store(item):
    pass


def process_category(item):
    total_category_list = []
    category_name_set = set()
    for category in item.menus:
        if not category['products']:
            continue
        category_name = category['name']
        if category_name in ['※注意事項']:
            continue
        category_name_set.add(category_name)
    for category_name in category_name_set:
        category_data = {
            'category_id': '',
            'category_name': category_name,
            'category_description': '',
            'category_image': ''
        }
        total_category_list.append(category_data)
    item.total_category_list = total_category_list


def save_to_db(item, conn):
    data = item.store_data
    cuisines_list = data.get('cuisines')
    cuisines_id = []
    cuisines_name = []
    cuisines_main = []
    if cuisines_list:
        for cc in cuisines_list:
            cuisines_id.append(str(cc.get('id')))
            cuisines_name.append(str(cc.get('name')))
            cuisines_main.append(str(cc.get('main')))
    food_characteristics_list = data.get('characteristics', {}).get('food_characteristics')
    food_characteristics_id = []
    food_characteristics_name = []
    food_characteristics_is_halal = []
    food_characteristics_is_vegetarian = []
    if food_characteristics_list:
        for fc in food_characteristics_list:
            food_characteristics_id.append(fc.get('id'))
            food_characteristics_name.append(fc.get('name'))
            food_characteristics_is_halal.append(str(fc.get('is_halal')))
            food_characteristics_is_vegetarian.append(str(fc.get('is_vegetarian')))

    schedules_list = data.get('schedules')
    schedules_map = {}
    if schedules_list:
        for sc in schedules_list:
            if sc.get('opening_type') == 'pickup':
                schedules_map[f'weekday_{sc.get("weekday")}_opening'] = sc.get('opening_time')
                schedules_map[f'weekday_{sc.get("weekday")}_closing'] = sc.get('closing_time')

    product_data_sql_list = (
        # 1-10
        item.batchNo, item.biz_type, data.get('web_path'), data.get('id'), data.get('code'),
        data.get('accepts_instructions'), data.get('address'), data.get('address_line2'), data.get('budget'),
        data.get('chain', {}).get('id'),
        # 11-20
        data.get('chain', {}).get('main_vendor_id'), data.get('chain', {}).get('name'),
        data.get('chain', {}).get('code'),
        data.get('city', {}).get('id'), data.get('city', {}).get('name'), '|'.join(cuisines_id),
        '|'.join(cuisines_name), '|'.join(cuisines_main),
        data.get('characteristics', {}).get('primary_cuisine', {}).get('id'),
        data.get('characteristics', {}).get('primary_cuisine', {}).get('name'),
        # 21-30
        str(data.get('characteristics', {}).get('primary_cuisine', {}).get('main')),
        '|'.join(food_characteristics_id), '|'.join(food_characteristics_name), '|'.join(food_characteristics_is_halal),
        '|'.join(food_characteristics_is_vegetarian), data.get('customer_phone'),
        data.get('hero_image'), data.get('hero_listing_image'), data.get('description'), str(data.get('is_active')),
        # 31-40
        str(data.get('is_checkout_comment_enabled', '')), data.get('is_new_until', ''),
        str(data.get('is_delivery_enabled', '')), str(data.get('is_pickup_enabled', '')),
        str(data.get('is_preorder_enabled', '')), str(data.get('is_service_fee_enabled', '')),
        str(data.get('is_service_tax_enabled', '')), str(data.get('is_service_tax_visible', '')),
        str(data.get('is_vat_disabled', '')), str(data.get('is_vat_included_in_product_price', '')),
        # 41-50
        str(data.get('is_vat_visible', '')), str(data.get('is_vat_included', '')), str(data.get('latitude', '')),
        str(data.get('location', '')), str(data.get('logo', '')), str(data.get('longitude', '')),
        str(data.get('food_license_number', '')), str(data.get('metadata', {}).get('timezone', '')),
        str(data.get('minimum_order_amount', '')), str(data.get('minimum_pickup_time', '')),
        # 51-60
        str(data.get('multiple_toppings', '')), str(data.get('name', '')), str(data.get('post_code', '')),
        str(data.get('rating', '')), str(data.get('review_number', '')), schedules_map.get('weekday_1_opening'),
        schedules_map.get('weekday_1_closing'), schedules_map.get('weekday_2_opening'),
        schedules_map.get('weekday_2_closing'), schedules_map.get('weekday_3_opening'),
        # 61-70
        schedules_map.get('weekday_3_closing'), schedules_map.get('weekday_4_opening'),
        schedules_map.get('weekday_4_closing'), schedules_map.get('weekday_5_opening'),
        schedules_map.get('weekday_5_closing'), schedules_map.get('weekday_6_opening'),
        schedules_map.get('weekday_6_closing'), schedules_map.get('weekday_7_opening'),
        schedules_map.get('weekday_7_closing'), str(data.get('service_fee', '')),
        # 71-80
        str(data.get('service_fee_percentage_amount', '')), str(data.get('service_tax_percentage_amount', '')),
        str(data.get('short_name', '')), str(data.get('small_order_fee', '')),
        str(data.get('vat_percentage_amount', '')), str(data.get('tag', '')), str(data.get('tags', '')),
        str(data.get('trade_register_number', '')), str(data.get('vendor_legal_information', {}).get('legal_name', '')),
        str(data.get('vendor_legal_information', {}).get('address_line_1', ''))

    )

    cur = conn.cursor()
    try:
        select_sql = """select count(1) as count from store_list where biz_type='food_panda' and code=? """
        cur.execute(select_sql, (item.store_code,))
        count = cur.fetchone()[0]
        if count <= 0:
            insert_many_sql = """INSERT INTO "store_list" (
            "batch_no", "biz_type", "web_path", "ID", "code", "accepts_instructions", "address", "address_line2", "budget", "chain_id", 
            "chain_main_vendor_id", "chain_name", "chain_code", "city_id", "city_name", "cuisines_id", "cuisines_name", "cuisines_main","primary_cuisine_id", "primary_cuisine_name",
             "primary_cuisine_main", "food_characteristics_id", "food_characteristics_name", "food_characteristics_is_halal", "food_characteristics_is_vegetarian", "customer_phone", "hero_image", "hero_listing_image", "description", "is_active",
             "is_checkout_comment_enabled", "is_new_until", "is_delivery_enabled", "is_pickup_enabled", "is_preorder_enabled", "is_service_fee_enabled", "is_service_tax_enabled", "is_service_tax_visible", "is_vat_disabled", "is_vat_included_in_product_price", 
            "is_vat_visible", "is_vat_included", "latitude", "location", "logo", "longitude", "food_license_number", "timezone", "minimum_order_amount", "minimum_pickup_time",
              "multiple_toppings", "name", "post_code", "rating", "review_number", "weekday_1_opening", "weekday_1_closing", "weekday_2_opening", "weekday_2_closing", "weekday_3_opening",
              "weekday_3_closing", "weekday_4_opening", "weekday_4_closing", "weekday_5_opening", "weekday_5_closing", "weekday_6_opening", "weekday_6_closing", "weekday_7_opening", "weekday_7_closing", "service_fee",
               "service_fee_percentage_amount", "service_tax_percentage_amount", "short_name", "small_order_fee", "vat_percentage_amount", "tag", "tags", "trade_register_number", "legal_name", "address_line_1") 
                                     VALUES (?,?,?,?,?,?,?,?,?,?,
                                     ?,?,?,?,?,?,?,?,?,?,
                                     ?,?,?,?,?,?,?,?,?,?,
                                     ?,?,?,?,?,?,?,?,?,?,
                                     ?,?,?,?,?,?,?,?,?,?,
                                     ?,?,?,?,?,?,?,?,?,?,
                                     ?,?,?,?,?,?,?,?,?,?,
                                     ?,?,?,?,?,?,?,?,?,?);"""
            cur.execute(insert_many_sql, product_data_sql_list)
            # print("save origin data:", cur.rowcount)
            conn.commit()
    except Exception as e:
        logging.error(str(e))
        return False
    finally:
        cur.close()


def save_to_merge_db(list, conn):
    cur = conn.cursor()
    try:
        insert_many_sql = """INSERT INTO "product_list_merge" ("batch_no", "biz_type","pos_product_id", "product_type", "name", 
                        "category", "sub_product_ids", "description", "price", "min", "max", "images")
                         VALUES (?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
        cur.executemany(insert_many_sql, list)
        print("save data after merge:", cur.rowcount)
        conn.commit()
    except Exception as e:
        logging.error(str(e))
        return False
    finally:
        cur.close()


def process_product(item):
    total_product_map = []
    for category in item.menus:
        category_name = category['name']
        if category_name in ['※注意事項']:
            continue
        source_product_list = category.get('products')
        if source_product_list is None:
            continue
        for p_idx, product in enumerate(source_product_list):
            product_id = str(product.get('id'))
            if not product_id:
                timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
                product_id = f'SI{p_idx}{timestamp}'
                product['id'] = product_id
            product_name = product.get('name')
            product_description = product.get('description')
            product_variations_list = product.get('product_variations')
            product_variations = product.get('product_variations')[0]
            product_price = fix_price(str(product_variations.get('price')))
            if product['file_path']:
                product_image = str(product['id']) + '.jpg'
            else:
                product_image = ''
            product_data = {'id': '',
                            'product_id': product_id,
                            'product_type': 'SINGLE',
                            'product_name': product_name,
                            'category_name': category_name,
                            'sub_product_ids': '',
                            'product_description': product_description,
                            'product_price': product_price,
                            'selection_range_min': '',
                            'selection_range_max': '',
                            'product_image': product_image,
                            'blockList': ''
                            }
            modifier_groups = []
            if len(product_variations_list) == 1:  # 只有 group-modifier
                topping_ids = product_variations.get('topping_ids')
                if topping_ids:
                    for topping_id in topping_ids:
                        topping = item.toppings.get(str(topping_id))
                        modifier_groups.append(topping)
            elif len(product_variations_list) > 1:  # 套餐 modifier-group-modifier
                variation = {'id': datetime.now().strftime("%Y%m%d%H%M%S"),
                             'name': '變化' if item.language == 'zh' else 'Variation',
                             'quantity_minimum': '1', 'quantity_maximum': '1'}
                v_options = []
                for pv in product_variations_list:
                    v_options.append(pv)
                variation['options'] = v_options
                modifier_groups.append(variation)

            if modifier_groups:
                product_data['modifier_groups'] = modifier_groups
                all_sub_group_id = set()
                for modifier_group in modifier_groups:
                    all_sub_group_id.add(str(modifier_group.get('id')))
                product_data['sub_product_ids'] = all_sub_group_id

            total_product_map.append(product_data)
    item.total_product_map = total_product_map


def process_sub_group(item):
    total_sub_group = item.total_sub_group
    sub_group_map = []
    for g_idx, modifier_group in enumerate(total_sub_group):
        group_id = str(modifier_group.get('id'))
        group_name = modifier_group.get('name')
        group_data = {'id': '',
                      'product_id': group_id,
                      'product_type': 'GROUP',
                      'product_name': group_name,
                      'category_name': '',
                      'sub_product_ids': '',
                      'product_description': '',
                      'product_price': '',
                      'selection_range_min': modifier_group.get('quantity_minimum'),
                      'selection_range_max': modifier_group.get('quantity_maximum'),
                      'product_image': '',
                      'blockList': ''
                      }
        modifiers = modifier_group.get('options')
        if modifiers:
            group_data['modifiers'] = modifiers
            all_sub_modifiers_id = set()
            for modifier in modifiers:
                all_sub_modifiers_id.add(str(modifier.get('id')))
            group_data['sub_product_ids'] = all_sub_modifiers_id

        sub_group_map.append(group_data)
    pass


def process_sub_item(item):
    total_group_map = item.total_sub_group
    sub_modifier_map = []
    for group_data in total_group_map:
        modifier_items = group_data.get('modifiers')
        if not modifier_items:
            continue
        for m_idx, modifier_item in enumerate(modifier_items):
            modifier_id = str(modifier_item.get('id'))
            modifier_name = modifier_item.get('name')

            tmp_price = str(modifier_item.get('price_before_discount', ''))
            if not tmp_price:
                tmp_price = str(modifier_item.get('price'))
            item_price = fix_price(tmp_price)
            result_item = {'id': '',
                           'product_id': modifier_id,
                           'product_type': 'MODIFIER',
                           'product_name': modifier_name,
                           'category_name': '',
                           'sub_product_ids': '',
                           'product_description': '',
                           'product_price': item_price,
                           'selection_range_min': '',
                           'selection_range_max': '',
                           'product_image': '',
                           'blockList': ''
                           }
            sub_modifier_map.append(result_item)
    item.total_sub_item = sub_modifier_map
    pass


def process_group(item):
    total_product_map = item.total_product_map
    total_group_map = []
    for product_data in total_product_map:
        modifier_groups = product_data.get('modifier_groups')
        if not modifier_groups:
            continue
        for g_idx, modifier_group in enumerate(modifier_groups):
            group_id = str(modifier_group.get('id'))
            group_name = modifier_group.get('name')
            group_data = {'id': '',
                          'product_id': group_id,
                          'product_type': 'GROUP',
                          'product_name': group_name,
                          'category_name': '',
                          'sub_product_ids': '',
                          'product_description': '',
                          'product_price': '',
                          'selection_range_min': modifier_group.get('quantity_minimum'),
                          'selection_range_max': modifier_group.get('quantity_maximum'),
                          'product_image': '',
                          'blockList': ''
                          }
            modifiers = modifier_group.get('options')
            if modifiers:
                group_data['modifiers'] = modifiers
                all_sub_modifiers_id = set()
                for modifier in modifiers:
                    all_sub_modifiers_id.add(str(modifier.get('id')))
                group_data['sub_product_ids'] = all_sub_modifiers_id

            total_group_map.append(group_data)
    item.total_group_map = total_group_map


def process_item(item):
    total_group_map = item.total_group_map
    total_modifier_map = []
    for group_data in total_group_map:
        modifier_items = group_data.get('modifiers')
        if not modifier_items:
            continue
        for m_idx, modifier_item in enumerate(modifier_items):
            modifier_id = str(modifier_item.get('id'))
            modifier_name = modifier_item.get('name')

            tmp_price = str(modifier_item.get('price_before_discount', ''))
            if not tmp_price:
                tmp_price = str(modifier_item.get('price'))
            item_price = fix_price(tmp_price)
            result_item = {'id': '',
                           'product_id': modifier_id,
                           'product_type': 'MODIFIER',
                           'product_name': modifier_name,
                           'category_name': '',
                           'sub_product_ids': '',
                           'product_description': '',
                           'product_price': item_price,
                           'selection_range_min': '',
                           'selection_range_max': '',
                           'product_image': '',
                           'blockList': ''
                           }
            if modifier_item.get('topping_ids'):  # single->modifier 包含 group
                topping_ids = modifier_item.get('topping_ids')
                for topping_id in topping_ids:
                    topping = item.toppings.get(str(topping_id))
                    group_id = str(topping.get('id'))
                    group_name = topping.get('name')
                    group_data = {'id': '',
                                  'product_id': group_id,
                                  'product_type': 'GROUP',
                                  'product_name': group_name,
                                  'category_name': '',
                                  'sub_product_ids': '',
                                  'product_description': '',
                                  'product_price': '',
                                  'selection_range_min': topping.get('quantity_minimum'),
                                  'selection_range_max': topping.get('quantity_maximum'),
                                  'product_image': '',
                                  'blockList': ''
                                  }
                    modifiers = topping.get('options')
                    if modifiers:
                        group_data['modifiers'] = modifiers
                        all_sub_modifiers_id = set()
                        for modifier in modifiers:
                            all_sub_modifiers_id.add(str(modifier.get('id')))
                        group_data['sub_product_ids'] = all_sub_modifiers_id

                    item.total_sub_group.append(group_data)
                all_sub_modifiers_id = set()
                for m_id in topping_ids:
                    all_sub_modifiers_id.add(str(m_id))
                result_item['sub_product_ids'] = all_sub_modifiers_id
            total_modifier_map.append(result_item)
            item.tree = True

    item.total_modifier_map = total_modifier_map


def replace_sub_id(_list, delete_ids):
    for p_id, data in _list.items():
        sub_product_ids = data.get('sub_product_ids')
        if not sub_product_ids:
            continue
        for _old, _new in delete_ids.items():
            new_category_name = _list.get(_new).get('category_name')
            if data.get('category_name') and new_category_name not in data['category_name']:
                data['category_name'] = '|'.join([data.get('category_name'), new_category_name])
            if _old in sub_product_ids.split('|'):
                sub_product_ids = sub_product_ids.replace(_old, _new)
        data['sub_product_ids'] = sub_product_ids
        _list[p_id] = data
    return _list


def build_key(data):
    key = '_'.join([data.get('product_type'), data.get('product_name'),
                    str(data.get('product_price', '')), str(data.get('selection_range_min', '')),
                    str(data.get('selection_range_max', ''))])
    return key


def update_sub_ids(product_type, ids_str, conn, batch_no):
    id_list = ids_str.split(',')
    if len(id_list) <= 1:
        return
    merge_id = id_list[0]
    try:
        for _idx, _id in enumerate(id_list):
            if _idx == 0:
                continue
            update_sub_ids_sql = """UPDATE product_list_merge SET 
            sub_product_ids=REPLACE(sub_product_ids,?,?) WHERE  batch_no=? and sub_product_ids LIKE ?;"""
            cur = conn.cursor()

            cur.execute(update_sub_ids_sql, (_id, merge_id, batch_no, f'%{_id}%'))
            # conn.set_trace_callback(logging.info)
        conn.commit()
    except Exception as e:
        logging.error(str(e))
        return False
    finally:
        cur.close()


def process_excel(item, conn):
    # if item.total_count != item.run_index + 1:
    #     return
    homedir = str(pathlib.Path.home())
    # total_category_list = item.total_category_list

    all_excel_data_product = [["web_path", "ID", "code", "accepts_instructions", "address", "address_line2", "budget",
                               "id", "main_vendor_id", "name", "code", "id", "name", "id", "name", "main", "id", "name",
                               "main", "id", "name", "is_halal", "is_vegetarian", "customer_phone", "hero_image",
                               "hero_listing_image", "description", "is_active", "is_checkout_comment_enabled",
                               "is_new_until", "is_delivery_enabled", "is_pickup_enabled", "is_preorder_enabled",
                               "is_service_fee_enabled", "is_service_tax_enabled", "is_service_tax_visible",
                               "is_vat_disabled", "is_vat_included_in_product_price", "is_vat_visible",
                               "is_vat_included",
                               "latitude", "location", "logo", "longitude", "food_license_number", "timezone",
                               "minimum_order_amount", "minimum_pickup_time", "multiple_toppings", "name", "post_code",
                               "rating", "review_number", "weekday_1_opening", "weekday_1_closing",
                               "weekday_2_opening", "weekday_2_closing", "weekday_3_opening", "weekday_3_closing",
                               "weekday_4_opening", "weekday_4_closing", "weekday_5_opening", "weekday_5_closing",
                               "weekday_6_opening", "weekday_6_closing", "weekday_7_opening", "weekday_7_closing",
                               "service_fee", "service_fee_percentage_amount", "service_tax_percentage_amount",
                               "short_name", "small_order_fee", "vat_percentage_amount", "tag", "tags",
                               "trade_register_number", "legal_name", "address_line_1"]]

    product_list_sql = """SELECT  "web_path", "ID", "code", "accepts_instructions", "address", "address_line2", "budget", "chain_id", 
            "chain_main_vendor_id", "chain_name", "chain_code", "city_id", "city_name", "cuisines_id", "cuisines_name", "cuisines_main","primary_cuisine_id", "primary_cuisine_name",
             "primary_cuisine_main", "food_characteristics_id", "food_characteristics_name", "food_characteristics_is_halal", "food_characteristics_is_vegetarian", "customer_phone", "hero_image", "hero_listing_image", "description", "is_active",
             "is_checkout_comment_enabled", "is_new_until", "is_delivery_enabled", "is_pickup_enabled", "is_preorder_enabled", "is_service_fee_enabled", "is_service_tax_enabled", "is_service_tax_visible", "is_vat_disabled", "is_vat_included_in_product_price", 
            "is_vat_visible", "is_vat_included", "latitude", "location", "logo", "longitude", "food_license_number", "timezone", "minimum_order_amount", "minimum_pickup_time",
              "multiple_toppings", "name", "post_code", "rating", "review_number", "weekday_1_opening", "weekday_1_closing", "weekday_2_opening", "weekday_2_closing", "weekday_3_opening",
              "weekday_3_closing", "weekday_4_opening", "weekday_4_closing", "weekday_5_opening", "weekday_5_closing", "weekday_6_opening", "weekday_6_closing", "weekday_7_opening", "weekday_7_closing", "service_fee",
               "service_fee_percentage_amount", "service_tax_percentage_amount", "short_name", "small_order_fee", "vat_percentage_amount", "tag", "tags", "trade_register_number", "legal_name", "address_line_1"
                FROM store_list where biz_type='food_panda';"""
    cur = conn.cursor()
    try:
        cur.execute(product_list_sql, )
        all_data_product = cur.fetchall()
    except Exception as e:
        logging.error(str(e))
        return False
    finally:
        cur.close()

    for dd in all_data_product:
        all_excel_data_product.append(
            [dd[0], dd[1], dd[2], dd[3], dd[4], dd[5], dd[6], dd[7], dd[8], dd[9],
             dd[10], dd[11], dd[12], dd[13], dd[14], dd[15], dd[16], dd[17], dd[18], dd[19],
             dd[20], dd[21], dd[22], dd[23], dd[24], dd[25], dd[26], dd[27], dd[28], dd[29],
             dd[30], dd[31], dd[32], dd[33], dd[34], dd[35], dd[36], dd[37], dd[38], dd[39],
             dd[40], dd[41], dd[42], dd[43], dd[44], dd[45], dd[46], dd[47], dd[48], dd[49],
             dd[50], dd[51], dd[52], dd[53], dd[54], dd[55], dd[56], dd[57], dd[58], dd[59],
             dd[60], dd[61], dd[62], dd[63], dd[64], dd[65], dd[66], dd[67], dd[68], dd[69],
             dd[70], dd[71], dd[72], dd[73], dd[74], dd[75], dd[76], dd[77],
             ])

    columns_sheet_product = [
        "", "data", "data", "data", "data", "data", "data",
        "chain", "chain", "chain", "chain", "city", "city", "cuisines", "cuisines", "cuisines", "primary_cuisine",
        "primary_cuisine",
        "primary_cuisine", "food_characteristics", "food_characteristics", "food_characteristics",
        "food_characteristics", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "",
        "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "",
        "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""
    ]

    xlsx_path = os.path.join(homedir, "Aim_menu", "store",
                             f"{item.batchNo}.xlsx")

    df = pd.DataFrame(all_excel_data_product, columns=columns_sheet_product)

    with pd.ExcelWriter(xlsx_path) as writer:
        df.to_excel(writer, sheet_name='storeList', index=False)

    print("Write file to " + xlsx_path)
    # print("Collection complete")

# if __name__ == '__main__':
#     test_url = 'https://www.foodpanda.hk/restaurant/v3iw/bakeout-homemade-koppepan'
#     parse_foodpanda(test_url)
