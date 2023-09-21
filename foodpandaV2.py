import json
import logging
import time
import os
import re
import requests
import pandas as pd
from datetime import datetime

from utils import isEn, init_path, fixStr, fix_price
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
    id = ''
    language = ''
    store_name = ''
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


def parse_foodpandaV2(page_url, variables):
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    batch_no = f'panda{timestamp}'
    print(f'run batch no:{batch_no}')

    root_data = fetch_data(page_url, variables)
    store_data = root_data.get('data', None)

    if store_data is None:
        print("Failure to parse menu data")
        return
    menus = store_data.get('menus', None)
    if menus is None:
        print("Failure to parse menu data")
        return
    item = FoodPandaItem()
    item.batchNo = batch_no
    item.id = store_data.get('ID')
    item.store_name = store_data.get('name')
    item.photoHref = store_data.get('photoHref')
    menus = store_data.get('menus', None)
    if menus is None:
        print("Failure to parse menu data")
        return
    item.menus = menus[0].get('menu_categories')
    item.toppings = menus[0].get('toppings')
    item.language = variables['language']

    clean_data(item)
    prepare_data(item)

    compose_images(item)
    if not '?img=no' in page_url:
        save_images(item)
    process_category(item)
    process_product(item)
    process_group(item)
    process_item(item)

    conn = sqlite3.connect(item.db_path)
    save_to_db(item, conn)
    process_final_list(item, conn)
    process_excel(item, conn)

    parse_foodpandaV1(item, variables)
    return True


def clean_data(item):
    homedir = str(pathlib.Path.home())
    store_path = os.path.join(homedir, "Aim_menu", item.biz_type, f"{fixStr(item.store_name.strip())}")
    if os.path.exists(store_path):
        shutil.rmtree(store_path)
    print("remove store dir")


def prepare_data(item):
    homedir = str(pathlib.Path.home())
    db_path = os.path.join(homedir, "Aim_menu", ".data.db")
    item.db_path = db_path
    if not os.path.exists(db_path):
        Path(db_path).touch()
        print("db init success！")
    conn = sqlite3.connect(db_path)
    conn.set_trace_callback(print)

    cur = conn.cursor()
    try:
        sql = """CREATE TABLE "product_list" (
                  "batch_no" varchar(64) NOT NULL,
                  "biz_type" varchar(16) NOT NULL,
                  "pos_product_id" varchar(64) NOT NULL,
                  "product_type" varchat(32) NOT NULL,
                  "name" varchar(255),
                  "category" varchar(255),
                  "sub_product_ids" text(2048),
                  "description" varchar(255),
                  "price" varchar(32),
                  "min" varchar(32),
                  "max" varchar(32),
                  "images" varchar(255),
                  "block_list" varchar(255)
                );"""
        sql2 = """CREATE TABLE "product_list_merge" (
                          "batch_no" varchar(64) NOT NULL,
                          "biz_type" varchar(16) NOT NULL,
                          "pos_product_id" varchar(64) NOT NULL,
                          "product_type" varchat(32) NOT NULL,
                          "name" varchar(255),
                          "category" varchar(255),
                          "sub_product_ids" text(2048),
                          "description" varchar(255),
                          "price" varchar(32),
                          "min" varchar(32),
                          "max" varchar(32),
                          "images" varchar(255),
                          "block_list" varchar(255)
                        );"""
        cur.execute(sql)
        cur.execute(sql2)
        print("create table success")
        return True
    except OperationalError as o:
        print(str(o))
        pass
        if str(o) == "table product_list already exists":
            return True
        return False
    except Exception as e:
        print(e)
        return False
    finally:
        cur.close()
        conn.close()

    pass


def fetch_data(page_url, variables):
    api_url = 'https://%s.fd-api.com/api/v5/vendors/%s?include=menus,bundles,multiple_discounts&language_id=6&basket_currency=TWD&show_pro_deals=true'
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
            print("Request to page, data being pulled")
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
    all_dir_path = init_path('food_panda', item.store_name, "ALL")
    for ca in item.menus:
        category_name = f"{fixStr(ca['name'])}"
        category_dir_path = init_path('food_panda', item.store_name, category_name)
        product_list = ca['products']
        for pd in product_list:
            if pd['file_path']:
                file_name = fixStr(pd['name'])
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
        r = requests.get(img['url'], timeout=180)
        file_name = img['file_name']
        image_path = os.path.join(img['category_dir_path'], file_name)
        image_path2 = os.path.join(img['all_dir_path'], file_name)
        blob = r.content
        with open(image_path, 'wb') as f:
            f.write(blob)
        with open(image_path2, 'wb') as f:
            f.write(blob)
        print("Download image: " + image_path)


def process_category(item):
    total_category_list = []
    category_name_set = set()
    for category in item.menus:
        if not category['products']:
            continue
        category_name = f"{fixStr(category['name'])}"
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
    total_product_map = item.total_product_map
    total_group_map = item.total_group_map
    total_modifier_map = item.total_modifier_map
    all_data_product = dict(**dict(**total_product_map, **total_group_map), **total_modifier_map)
    product_data_sql_list = []
    for dd in all_data_product.values():
        product_data_sql = (
            item.batchNo, item.biz_type, dd.get('product_id'), dd.get('product_type'), dd.get('product_name'),
            dd.get('category_name'), ','.join(dd.get('sub_product_ids')), dd.get('product_description'),
            dd.get('product_price'), dd.get('selection_range_min'), dd.get('selection_range_max'),
            dd.get('product_image'))
        product_data_sql_list.append(product_data_sql)
    cur = conn.cursor()
    try:
        insert_many_sql = """INSERT INTO "product_list" ("batch_no", "biz_type","pos_product_id", "product_type", "name", 
                        "category", "sub_product_ids", "description", "price", "min", "max", "images")
                         VALUES (?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
        cur.executemany(insert_many_sql, product_data_sql_list)
        print("save origin data:", cur.rowcount)
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
    total_product_map = {}
    for category in item.menus:
        category_name = f"{fixStr(category['name'])}"
        source_product_list = category.get('products')
        if source_product_list is None:
            continue
        for p_idx, product in enumerate(source_product_list):
            product_id = str(product.get('id'))
            if not product_id:
                timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
                product_id = f'SI{p_idx}{timestamp}'
                product['id'] = product_id
            product_name = fixStr(product.get('name'))
            product_description = product.get('description')
            product_variations = product.get('product_variations')[0]
            product_price = fix_price(str(product_variations.get('price')))
            if product['file_path']:
                product_image = fixStr(product['name']) + '.jpg'
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
            topping_ids = product_variations.get('topping_ids')
            modifier_groups = []
            if topping_ids:
                for topping_id in topping_ids:
                    topping = item.toppings.get(str(topping_id))
                    modifier_groups.append(topping)

            if modifier_groups:
                product_data['modifier_groups'] = modifier_groups
                all_sub_group_id = set()
                for modifier_group in modifier_groups:
                    all_sub_group_id.add(str(modifier_group.get('id')))
                product_data['sub_product_ids'] = all_sub_group_id

            total_product_map[product_id] = product_data
    item.total_product_map = total_product_map


def process_group(item):
    total_product_map = item.total_product_map
    total_group_map = {}
    for product_data in total_product_map.values():
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

            total_group_map[group_id] = group_data
    item.total_group_map = total_group_map


def process_item(item):
    total_group_map = item.total_group_map
    total_modifier_map = {}
    for group_data in total_group_map.values():
        modifier_items = group_data.get('modifiers')
        if not modifier_items:
            continue
        for m_idx, modifier_item in enumerate(modifier_items):
            modifier_id = str(modifier_item.get('id'))
            modifier_name = modifier_item.get('name')

            item_price = fix_price(str(modifier_item.get('price')))
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
            total_modifier_map[modifier_id] = result_item

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
            sub_product_ids=REPLACE(sub_product_ids,?,?) WHERE  batch_no=? and product_type=? and sub_product_ids LIKE ?;"""
            cur = conn.cursor()

            cur.execute(update_sub_ids_sql, (_id, merge_id, batch_no, product_type, f'%{_id}%'))
            # conn.set_trace_callback(logging.info)
        conn.commit()
    except Exception as e:
        logging.error(str(e))
        return False
    finally:
        cur.close()


# 爬虫去重逻辑
# 1. (single) name+price 去重
# 2. (group) name+min+max 去重（如果可以，就通过blocklist实现 - P1）
# 3. (modifier) name+price 去重（如果可以，就通过blocklist实现 - P1）

def process_final_list(item, conn):
    # product
    product_list_sql = """SELECT DISTINCT batch_no, biz_type,pos_product_id,product_type,name, 
    group_concat(category,'|') as category,
    group_concat(DISTINCT sub_product_ids) as sub_product_ids,description,price,min,max,images FROM product_list WHERE batch_no=? and product_type='SINGLE' 
    GROUP BY name,price;  """
    cur = conn.cursor()
    try:
        cur.execute(product_list_sql, (item.batchNo,))
        product_list = cur.fetchall()
    except Exception as e:
        logging.error(str(e))
        return False
    finally:
        cur.close()
    save_to_merge_db(product_list, conn)

    # group
    group_list_sql = """SELECT DISTINCT group_concat(DISTINCT pos_product_id) as pos_product_id_merge,batch_no,
    biz_type,pos_product_id,product_type,name,  
    category,group_concat(DISTINCT sub_product_ids) as sub_product_ids,description,price,min,max,images 
    FROM product_list WHERE batch_no=? and product_type='GROUP' GROUP BY name,min,max;  """

    cur = conn.cursor()
    try:
        cur.execute(group_list_sql, (item.batchNo,))
        group_list = cur.fetchall()
        fixed_group_list = []
        for gg in group_list:
            pos_product_ids = gg[0]
            id_list = pos_product_ids.split(',', 1)
            if len(id_list) == 2:
                update_sub_ids('SINGLE', pos_product_ids, conn, item.batchNo)
            fixed_group_list.append(
                (gg[1], gg[2], gg[3], gg[4], gg[5], gg[6], gg[7], gg[8], gg[9], gg[10], gg[11], gg[12]))
    except Exception as e:
        logging.error(str(e))
        return False
    finally:
        cur.close()
    save_to_merge_db(fixed_group_list, conn)
    # modifier
    modifier_list_sql = """SELECT DISTINCT group_concat(DISTINCT pos_product_id) as pos_product_id_merge,batch_no, biz_type,pos_product_id,product_type,name,  
        category,sub_product_ids,description,price,min,max,images 
        FROM product_list WHERE batch_no=? and product_type='MODIFIER' GROUP BY name,price;"""

    cur = conn.cursor()
    try:
        cur.execute(modifier_list_sql, (item.batchNo,))
        modifier_list = cur.fetchall()
        fixed_modifier_list = []
        for gg in modifier_list:
            pos_product_ids = gg[0]
            id_list = pos_product_ids.split(',', 1)
            if len(id_list) == 2:
                update_sub_ids('GROUP', pos_product_ids, conn, item.batchNo)
            fixed_modifier_list.append(
                (gg[1], gg[2], gg[3], gg[4], gg[5], gg[6], gg[7], gg[8], gg[9], gg[10], gg[11], gg[12]))
    except Exception as e:
        logging.error(str(e))
        return False
    finally:
        cur.close()
    save_to_merge_db(fixed_modifier_list, conn)


def process_excel(item, conn):
    homedir = str(pathlib.Path.home())
    total_category_list = item.total_category_list

    all_excel_data_product = [
        ['This row contain the description and instruction of each fields,To import data, please start on line 3',
         '(REQUIRED) This is your unique product ID of SINGLE or MODIFIER.',
         'Indicate where this is an SINGLE, MODIFIER or GROUP. Single refers to the main item. Group refers to Modifier group.',
         '(REQUIRED) The name of the SINGLE, MODIFIER or GROUP. Max 32 characters.',
         '(REQUIRED) The name of the category for the item. The name need to correspond with the category in \'categoryList\' or an existing category in backoffice.',
         '(OPTIONAL) The PosProductIDs of the item modifiers that should be a subset. Use | to separate multiple IDs.',
         '(OPTIONAL) The description of SINGLE, MODIFIER or GROUP. Max 64 characters',
         '(REQUIRED) The price of the Item or Modifier. Leave blank for GROUP.',
         '(REQUIRED) The minimum number of options to select for GROUP. Leave blank for SINGLE and MODIFIER.',
         '(REQUIRED) The maximum number of options to select for GROUP. Leave blank for SINGLE and MODIFIER.',
         '(OPTIONAL) The file name of the image for this SINGLE or MODIFIER. Ensure it correspond with the actual image file.',
         '(OPTIONAL) The posProductID of the MODIFIER to be excluded from an item. Use | to separate multiple IDs.']]
    all_excel_data_category = [
        ['This row contain the description and instruction of each fields,To import data, please start on line 3',
         '(REQUIRED) Fill in the new categories to create. The name need to correspond with the \'category\' under productList. Only for new categories. Max 32 characters.',
         '(OPTIONAL) The description of category. Max 64 characters.',
         '(OPTIONAL) The file name of the image for this SINGLE or MODIFIER. Ensure it correspond with the actual image file.']]
    all_excel_data_lang_zh = [
        ['This row contain the description and instruction of each fields,To import data, please start on line 3',
         '(REQUIRED) This is your unique product ID of SINGLE or MODIFIER.The ID needs to correspond with the \'posProductId\' under productList.',
         '(REQUIRED) The name of the SINGLE, MODIFIER or GROUP in Chinese. Max 32 characters.',
         '(OPTIONAL) The description of SINGLE, MODIFIER or GROUP in Chinese. Max 64 characters. Only required if you have input a description under productList']]
    all_excel_data_lang_th = [
        ['This row contain the description and instruction of each fields,To import data, please start on line 3',
         '(REQUIRED) This is your unique product ID of SINGLE or MODIFIER.The ID needs to correspond with the \'posProductId\' under productList.',
         '(REQUIRED) The name of the SINGLE, MODIFIER or GROUP in Thai. Max 32 characters.',
         '(OPTIONAL) The description of SINGLE, MODIFIER or GROUP in Thai. Max 64 characters. Only required if you have input a description under productList']]
    all_excel_data_lang_ms = [
        ['This row contain the description and instruction of each fields,To import data, please start on line 3',
         '(REQUIRED) This is your unique product ID of SINGLE or MODIFIER.The ID needs to correspond with the \'posProductId\' under productList.',
         '(REQUIRED) The name of the SINGLE, MODIFIER or GROUP in Malay. Max 32 characters.',
         '(OPTIONAL) The description of SINGLE, MODIFIER or GROUP in Malay. Max 64 characters. Only required if you have input a description under productList']]
    all_excel_data_lang_en = [
        ['This row contain the description and instruction of each fields,To import data, please start on line 3',
         '(REQUIRED) This is your unique product ID of SINGLE or MODIFIER.The ID needs to correspond with the \'posProductId\' under productList.',
         '(REQUIRED) The name of the SINGLE, MODIFIER or GROUP in English. Max 32 characters.',
         '(OPTIONAL) The description of SINGLE, MODIFIER or GROUP in English. Max 64 characters. Only required if you have input a description under productList']]

    product_list_sql = """SELECT pos_product_id,product_type,name,category,REPLACE(sub_product_ids,',','|') as sub_product_ids,description,price,min,max,images,block_list FROM product_list_merge WHERE batch_no=?;"""
    cur = conn.cursor()
    try:
        cur.execute(product_list_sql, (item.batchNo,))
        all_data_product = cur.fetchall()
    except Exception as e:
        logging.error(str(e))
        return False
    finally:
        cur.close()

    for dd in all_data_product:
        all_excel_data_product.append(
            ['', dd[0], dd[1], dd[2], dd[3], dd[4], dd[5], dd[6], dd[7], dd[8], dd[9], ''])

    for cc in total_category_list:
        all_excel_data_category.append(
            [cc.get('category_id'), cc.get('category_name'), cc.get('category_description'),
             cc.get('category_image')])

    columns_sheet_category = ["categoryID", "categoryName", "categoryDescription",
                              "categoryimage"]

    columns_sheet_product = ["productId", "posProductId", "productType", "name",
                             "category", "subPosProductIds", "description",
                             "price", "min", "max", "images",
                             "blockList"]

    columns_sheet_lang = ["ProductId", "posProductId", "name", "description"]

    xlsx_path = os.path.join(homedir, "Aim_menu", "food_panda",
                             f"{item.store_name}_{item.language}_V2.xlsx")

    df1 = pd.DataFrame(all_excel_data_category, columns=columns_sheet_category)
    df2 = pd.DataFrame(all_excel_data_product, columns=columns_sheet_product)
    df3 = pd.DataFrame(all_excel_data_lang_zh, columns=columns_sheet_lang)
    df4 = pd.DataFrame(all_excel_data_lang_th, columns=columns_sheet_lang)
    df5 = pd.DataFrame(all_excel_data_lang_ms, columns=columns_sheet_lang)
    df6 = pd.DataFrame(all_excel_data_lang_en, columns=columns_sheet_lang)

    with pd.ExcelWriter(xlsx_path) as writer:
        df1.to_excel(writer, sheet_name='categoryList', index=False)
        df2.to_excel(writer, sheet_name='productList', index=False)
        df3.to_excel(writer, sheet_name='zh-CN', index=False)
        df4.to_excel(writer, sheet_name='th-TH', index=False)
        df5.to_excel(writer, sheet_name='ms-MY', index=False)
        df6.to_excel(writer, sheet_name='en-US', index=False)

    print("Write file to " + xlsx_path)
    # print("Collection complete")


def parse_foodpandaV1(item, variables):
    homedir = str(pathlib.Path.home())
    menu_categories = item.menus

    if menu_categories is None:
        print("Failure to parse menu category data")
        return

    toppings = item.toppings
    # print(toppings)
    food_panda_list = []
    store_name = item.store_name
    for category in menu_categories:
        products_list = category.get('products', None)
        full_category_name = category['name']
        category_name = re.sub(r'[:/\\?*“”<>|""]', '_', full_category_name)
        full_category_descrption = category['description']
        category_description = re.sub(r'[:/\\?*“”<>|""]', '_', full_category_descrption)
        # os.path.join("..", "Aim_menu", "food_panda", f"{store_name}", f"{category_name}")
        dirPath = os.path.join(homedir, "Aim_menu", "food_panda", f"{store_name.strip()}", f"{category_name.strip()}")
        if not os.path.exists(dirPath):
            os.makedirs(dirPath)
        if products_list is None:
            print("category：{name}，no information".format(name=category['name']))
            continue
        for product in products_list:
            # result = {}
            # result.clear()
            item_name = re.sub(r'[:/\\?*“”<>|""]', '_', product.get('name'))
            total_category = category_name
            total_category_descrption = category_description
            total_item_name = item_name
            total_description = product.get('description')
            product_variations = product.get('product_variations')
            total_image = '' if product['images'] == [] else product['images'][0]['image_url']
            item_image_name = ''
            if total_image != '':
                item_image_name = f"{fixStr(item_name.strip())}.jpg"
            if product_variations is None:
                print("product ：{name}，no information".format(name=product.get('name')))
                continue
            if len(product_variations) == 1:
                logging.info("only one product ：{name}，no information".format(name=product.get('name')))
            elif len(product_variations) > 1:
                logging.info("multiple products ：{name}".format(name=product.get('name')))
                for pv in product_variations:
                    total_package_type = pv.get('name', '')
                    total_package_price = pv.get('price')
                    result = {'category': total_category, 'category_description': total_category_descrption,
                              'item_name': total_item_name, 'description': total_description, 'modifier_group': 'Combo',
                              'package_price': product_variations[0].get('price'), 'options': total_package_type,
                              'options_price': total_package_price, 'select_type': 'Single', 'required_or_not': 'TRUE',
                              'min_available': 1, 'max_available': 1}
                    food_panda_list.append(result)

            for pv in product_variations:
                total_package_type = pv.get('name', '')
                total_package_price = pv.get('price')
                result = {}
                result['category'] = total_category
                result['category_description'] = total_category_descrption
                result['item_name'] = total_item_name
                result['item_image'] = item_image_name
                result['description'] = total_description
                result['package_type'] = total_package_type
                result['package_price'] = total_package_price
                # result['modifier_group'] = total_modifier_group
                # result['select_type'] = total_select_type
                # result['required_or_not'] = total_required_or_not
                # result['min_available'] = total_min_available
                # result['max_available'] = total_max_available
                # result['options'] = total_options
                # result['options_price'] = total_options_price
                topping_ids = pv['topping_ids']
                # result = {}
                # result['category'] = total_category
                # result['category_description'] = total_category_descrption
                # result['modifier_group'] = total_package_type
                # result['options'] = total_package_type
                # food_panda_list.append(result)
                if topping_ids:
                    for topping_id in topping_ids:
                        topping = toppings.get(str(topping_id))
                        total_modifier_group = topping.get('name')
                        total_required_or_not = 'TRUE' if topping.get('quantity_minimum') > 0 else 'FALSE'
                        if topping.get('quantity_minimum') == topping.get('quantity_maximum') == 1:
                            total_select_type = 'Single'
                        else:
                            total_select_type = 'Multiple'

                        total_min_available = topping.get('quantity_minimum')
                        total_max_available = topping.get('quantity_maximum')
                        options = topping['options']
                        for op in options:
                            total_options = op.get('name')
                            total_options_price = op.get('price')
                            result = {}
                            result['category'] = total_category
                            result['category_description'] = total_category_descrption
                            result['item_name'] = total_item_name
                            result['item_image'] = item_image_name
                            result['description'] = total_description
                            result['package_type'] = total_package_type
                            result['package_price'] = total_package_price
                            result['modifier_group'] = total_modifier_group
                            result['select_type'] = total_select_type
                            result['required_or_not'] = total_required_or_not
                            result['min_available'] = total_min_available
                            result['max_available'] = total_max_available
                            result['options'] = total_options
                            result['options_price'] = total_options_price
                            food_panda_list.append(result)
                else:
                    food_panda_list.append(result)

    food_panda_excel_list = []
    i = 0
    while i < len(food_panda_list):
        excel_language = variables.get('language', 'EN')
        excel_outlet_id = ''
        excel_outlet_services = ''
        excel_over_write = ''
        excel_category_name_en = (food_panda_list[i]).get('category') if isEn(variables) else ''
        excel_category_name_th = (food_panda_list[i]).get('category') if isTh(variables) else ''
        excel_category_name_cn = (food_panda_list[i]).get('category') if isCn(variables) else ''
        excel_category_sku = ''
        excel_category_description_en = (food_panda_list[i]).get('category_description') if isEn(variables) else ''
        excel_category_description_th = (food_panda_list[i]).get('category_description') if isTh(variables) else ''
        excel_category_description_cn = (food_panda_list[i]).get('category_description') if isCn(variables) else ''
        excel_item_name_en = (food_panda_list[i]).get('item_name') if isEn(variables) else ''
        excel_item_name_th = (food_panda_list[i]).get('item_name') if isTh(variables) else ''
        excel_item_name_cn = (food_panda_list[i]).get('item_name') if isCn(variables) else ''
        excel_item_sku = ''
        excel_item_image = (food_panda_list[i]).get('item_image')
        excel_description_en = (food_panda_list[i]).get('description') if isEn(variables) else ''
        excel_description_th = (food_panda_list[i]).get('description') if isTh(variables) else ''
        excel_description_cn = (food_panda_list[i]).get('description') if isCn(variables) else ''
        excel_conditional_modifier = (food_panda_list[i]).get('package_type')
        excel_item_price = (food_panda_list[i]).get('package_price')

        excel_modifier_group_en = (food_panda_list[i]).get('modifier_group') if isEn(variables) else ''
        excel_modifier_group_th = (food_panda_list[i]).get('modifier_group') if isTh(variables) else ''
        excel_modifier_group_cn = (food_panda_list[i]).get('modifier_group') if isCn(variables) else ''
        excel_modifier_group_sku = ''
        excel_modifier_group_description_en = ''
        excel_modifier_group_description_th = ''
        excel_modifier_group_description_cn = ''
        excel_select_type = (food_panda_list[i]).get('select_type')
        excel_required_or_not = (food_panda_list[i]).get('required_or_not')
        excel_min_available = (food_panda_list[i]).get('min_available')
        excel_max_available = (food_panda_list[i]).get('max_available')

        excel_modifier_en = (food_panda_list[i]).get('options') if isEn(variables) else ''
        excel_modifier_th = (food_panda_list[i]).get('options') if isTh(variables) else ''
        excel_modifier_cn = (food_panda_list[i]).get('options') if isCn(variables) else ''
        excel_modifier_sku = ''
        excel_modifier_description_en = ''
        excel_modifier_description_th = ''
        excel_modifier_description_cn = ''
        excel_options_price = (food_panda_list[i]).get('options_price')

        food_panda_excel_list.append(
            [excel_language, excel_outlet_id, excel_outlet_services, excel_over_write,
             excel_category_name_en, excel_category_name_th, excel_category_name_cn, excel_category_sku,
             excel_category_description_en, excel_category_description_th, excel_category_description_cn,
             excel_item_name_en, excel_item_name_th, excel_item_name_cn, excel_item_sku, excel_item_image,
             excel_description_en, excel_description_th, excel_description_cn,
             excel_conditional_modifier, excel_item_price,
             excel_modifier_group_en, excel_modifier_group_th, excel_modifier_group_cn, excel_modifier_group_sku,
             excel_modifier_group_description_en, excel_modifier_group_description_th,
             excel_modifier_group_description_cn,
             excel_select_type, excel_required_or_not, excel_min_available, excel_max_available,
             excel_modifier_en, excel_modifier_th, excel_modifier_cn, excel_modifier_sku,
             excel_modifier_description_en, excel_modifier_description_th, excel_modifier_description_cn,
             excel_options_price, '', '', '', '', ''])
        i += 1
    #     	Outlet ID

    df = pd.DataFrame(food_panda_excel_list,
                      columns=["Language", "Outlet ID", "Outlet services", "Overwrite (Y/N)", "category_name_en",
                               "category_name_th",
                               "category_name_cn", "category_sku", "category_description_en",
                               "category_description_th",
                               "category_description_cn", "item_name_en", "item_name_th", "item_name_cn", "item_sku",
                               "item_image", "description_en", "description_th", "description_cn",
                               "conditional_modifier", "item_price", "modifier_group_en",
                               "modifier_group_th", "modifier_group_cn", "modifier_group_sku",
                               "modifier_group_description_en", "modifier_group_description_th",
                               "modifier_group_description_cn", "select_type",
                               "required_or_not", "min_available", "max_available", "modifier_en",
                               "modifier_th", "modifier_cn", "modifier_sku", "modifier_description_en",
                               "modifier_description_th", "modifier_description_cn", "options_price", "open_field1",
                               "open_field2", "open_field3", "open_field4", "open_field5"])
    df.index = range(1, len(df) + 1)
    xlsx_path = os.path.join(homedir, "Aim_menu", "food_panda", f"{store_name}_{variables['language']}.xlsx")
    if os.path.exists(xlsx_path):
        os.remove(xlsx_path)
    print("Write file to " + xlsx_path)
    df.to_excel(xlsx_path, index=False)
    # print("Collection complete")
    return True

# if __name__ == '__main__':
#     test_url = 'https://www.foodpanda.hk/restaurant/v3iw/bakeout-homemade-koppepan'
#     parse_foodpanda(test_url)
