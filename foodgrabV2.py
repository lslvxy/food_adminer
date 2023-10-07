import json
import logging
import os
import re
import urllib
from datetime import datetime
from sqlite3 import OperationalError
import sqlite3
import shutil
from pathlib import Path

import pandas as pd
import requests
import time
from bs4 import BeautifulSoup

from utils import isEn, init_path, fix_price
from utils import isCn
from utils import isTh
from utils import fixStr
from utils import toExcel

import pathlib

bc = logging.basicConfig(level=logging.INFO, format='%(asctime)s  - %(message)s')


class FoodGrabItem:
    biz_type = 'food_grab'
    id = ''
    language = ''
    store_name = ''
    photoHref = ''
    image_list = []
    categories = []
    menus = []
    total_category_list = []
    total_product_map = {}
    total_group_map = {}
    total_modifier_map = {}
    fina_data = []


def parse_foodgrabV2(page_url, variables):
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    batch_no = f'grab{timestamp}'
    print(f'run batch no:{batch_no}')
    logging.info(f'run batch no:{batch_no}')

    store_data = fetch_data(page_url, variables)
    item = FoodGrabItem()
    item.batchNo = batch_no
    item.id = store_data.get('ID')
    item.store_name = store_data.get('name')
    item.photoHref = store_data.get('photoHref')
    category_list = store_data.get('menu').get('categories')
    item.menus = category_list
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
    parse_foodgrabV1(item, variables)
    return True


def clean_data(item):
    homedir = str(pathlib.Path.home())
    store_path = os.path.join(homedir, "Aim_menu", item.biz_type, f"{fixStr(item.store_name.strip())}")
    if os.path.exists(store_path):
        shutil.rmtree(store_path)
    print("remove store dir")


def prepare_data(item):
    homedir = str(pathlib.Path.home())
    dir_path = os.path.join(homedir, "Aim_menu")
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


def fetch_html(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36 Edg/113.0.1774.50',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br'
    }
    payload = {}
    response = requests.request("GET", url, headers=headers, data=payload)

    i = 0
    while i < 10:
        if response.status_code == 200:
            print("Request to page, data being pulled")
            break
        i += 1
        print("Page response failed, retrying")
        time.sleep(5)
        response = requests.request("GET", url, headers=headers, data=payload)
    if i == 10 and response.status_code != 200:
        print("Page response failed, please check network link and try again later")
        return None
    text = response.text
    # logging.info(text)
    soup = BeautifulSoup(text, 'html.parser')
    return soup


def fetch_data(page_url, variables):
    soup = fetch_html(page_url)
    store_id = variables['id']
    next_data = json.loads(soup.find("script", id="__NEXT_DATA__").get_text())
    store_data = next_data.get('props').get('initialReduxState').get('pageRestaurantDetail').get(
        'entities').get(store_id)
    return store_data


def compose_images(item):
    image_list = []
    store_name = fixStr(item.store_name)
    all_dir_path = init_path('food_grab', store_name, "ALL")
    for ca in item.menus:
        category_name = f"{fixStr(ca['name'])}"
        category_dir_path = init_path('food_grab', store_name, category_name)
        product_list = ca['items']
        for pd in product_list:
            if pd['imgHref']:
                file_name = fixStr(pd['name'])
                image_list.append({
                    'category': category_name,
                    'all_dir_path': all_dir_path,
                    'category_dir_path': category_dir_path,
                    'url': pd['imgHref'],
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
        if not category['available']:
            continue
        category_name = category['name']
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
    # all_data_product = dict(**dict(**total_product_map, **total_group_map), **total_modifier_map)
    all_data_product = total_product_map + total_group_map + total_modifier_map
    product_data_sql_list = []
    for dd in all_data_product:
        product_data_sql = (
            item.batchNo, item.biz_type, dd.get('product_id'), dd.get('product_type'), dd.get('product_name'),
            urllib.parse.quote_plus(dd.get('category_name')), ','.join(dd.get('sub_product_ids')),
            dd.get('product_description'),
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
    total_product_map = []
    for category in item.menus:
        category_name =category['name']
        source_product_list = category.get('items')
        if source_product_list is None:
            continue
        for p_idx, product in enumerate(source_product_list):
            product_id = product.get('ID')
            if not product_id:
                timestamp = datetime.timestamp(datetime.now())
                product_id = f'SI{p_idx}{timestamp}'
                product['ID'] = product_id
            product_name = product.get('name')

            product_description = product.get('description')
            product_price = fix_price(str(product.get('priceV2').get('amountDisplay')))
            if product['imgHref']:
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

            modifier_groups = product.get('modifierGroups')
            if modifier_groups:
                product_data['modifier_groups'] = modifier_groups
                all_sub_group_id = set()
                for modifier_group in modifier_groups:
                    all_sub_group_id.add(modifier_group.get('ID'))
                product_data['sub_product_ids'] = all_sub_group_id

            total_product_map.append(product_data)

    item.total_product_map = total_product_map


def process_group(item):
    total_product_map = item.total_product_map
    total_group_map = []
    for product_data in total_product_map:
        modifier_groups = product_data.get('modifier_groups')
        if not modifier_groups:
            continue
        for g_idx, modifier_group in enumerate(modifier_groups):
            group_id = modifier_group.get('ID')
            if not group_id:
                timestamp = datetime.timestamp(datetime.now())
                group_id = f'GO{product_data.get("product_id")}{g_idx}{timestamp}'
            group_name = modifier_group.get('name')
            group_data = {'id': '',
                          'product_id': group_id,
                          'product_type': 'GROUP',
                          'product_name': group_name,
                          'category_name': '',
                          'sub_product_ids': '',
                          'product_description': '',
                          'product_price': '',
                          'selection_range_min': modifier_group.get('selectionRangeMin'),
                          'selection_range_max': modifier_group.get('selectionRangeMax'),
                          'product_image': '',
                          'blockList': ''
                          }
            modifiers = modifier_group.get('modifiers')
            if modifiers:
                group_data['modifiers'] = modifiers
                all_sub_modifiers_id = set()
                for modifier in modifiers:
                    all_sub_modifiers_id.add(modifier.get('ID'))
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
            modifier_id = modifier_item.get('ID')
            if not modifier_id:
                timestamp = datetime.timestamp(datetime.now())
                modifier_id = f'GO{group_data.get("product_id")}{m_idx}{timestamp}'
            modifier_name = modifier_item.get('name')

            item_price = fix_price(str(modifier_item.get('priceV2').get('amountDisplay')))
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
            total_modifier_map.append(result_item)

    item.total_modifier_map = total_modifier_map


def find_by_id(list, name):
    for c in list:
        if c.get('product_id') == name:
            return c


def merge_sub_id(_list, delete_ids):
    for _old, _new in delete_ids.items():
        # delete old data
        _old_data = _list[_old]
        _new_data = _list[_new]
        _old_product_ids = _list[_old].get('sub_product_ids')
        if not _old_product_ids:
            _old_product_ids = {}
        _new_product_ids = _list[_new].get('sub_product_ids')
        if not _new_product_ids:
            _new_product_ids = {}
        _list[_new]['sub_product_ids'] = _old_product_ids | _new_product_ids
    return _list


def replace_sub_id(_list, delete_ids):
    for p_id, data in _list.items():
        sub_product_ids = data.get('sub_product_ids')
        if not sub_product_ids:
            continue
        # for _old, _new in delete_ids.items():
        #     new_category_name = _list.get(_new).get('category_name')
        #     if data.get('category_name') and new_category_name not in data['category_name']:
        #         data['category_name'] = '|'.join([data.get('category_name'), new_category_name])
        #         _list[p_id] = data
        for _old, _new in delete_ids.items():
            # new_category_name = _list.get(_new).get('category_name')
            # if data.get('category_name') and new_category_name not in data['category_name']:
            #     data['category_name'] = '|'.join([data.get('category_name'), new_category_name])
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
    GROUP_CONCAT(DISTINCT category) as category,
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

    product_list_sql = """SELECT pos_product_id,product_type,name,REPLACE(category,',','|') 
        as category,sub_product_ids,description,price,min,max,images,block_list
        FROM product_list_merge WHERE batch_no=?;"""
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
        sub_product_ids = dd[4]
        sub_id_list = sub_product_ids.split(',')
        sub_product_ids = "|".join(set(sub_id_list))
        all_excel_data_product.append(
            ['', dd[0], dd[1], dd[2], urllib.parse.unquote_plus(dd[3]), sub_product_ids, dd[5], dd[6], dd[7], dd[8],
             dd[9], ''])
    for cc in total_category_list:
        all_excel_data_category.append(
            [cc.get('category_id'), cc.get('category_name'), cc.get('category_description'),
             cc.get('category_image')])

    columns_sheet_category = ["categoryId", "categoryName", "categoryDescription",
                              "categoryImage"]

    columns_sheet_product = ["productId", "posProductId", "productType", "name",
                             "category", "subPosProductIds", "description",
                             "price", "min", "max", "images",
                             "blockList"]

    columns_sheet_lang = ["productId", "posProductId", "name", "description"]
    xlsx_path = os.path.join(homedir, "Aim_menu", "food_grab",
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


def parse_foodgrabV1(item, variables):
    homedir = str(pathlib.Path.home())
    store_name = item.store_name
    category_list = item.menus
    if category_list is None:
        return None

    food_grab_list = []
    for category in category_list:
        category_name = category.get('name')
        product_list = category.get('items')
        if product_list is None:
            continue
        for product in product_list:
            product_id = product.get('ID')
            product_name = product.get('name')
            # logging.info("parse product_name: " + product_name)
            product_description = product.get('description')
            product_price = product.get('priceV2').get('amountDisplay')
            product_image = fixStr(product_name) + '.jpg'
            result = {'product_id': product_id, 'category_name': category_name,
                      'product_name': product_name, 'product_description': product_description,
                      'product_price': product_price, 'product_image': product_image}

            modifier_groups = product.get('modifierGroups')
            if modifier_groups:
                for modifier_group in modifier_groups:
                    group_name = modifier_group.get('name')
                    # logging.info("parse group_name: " + product_name)
                    modifier_items = modifier_group.get('modifiers')
                    group_select_type = 'Single' if modifier_group.get('selectionType') == 0 else 'Multiple'
                    if modifier_group.get('selectionRangeMin') == modifier_group.get('selectionRangeMax') == 1:
                        group_required_or_not = "TRUE"
                    else:
                        group_required_or_not = "FALSE"
                    group_min_available = modifier_group.get('selectionRangeMin')
                    group_max_available = modifier_group.get('selectionRangeMax')

                    if modifier_items:
                        for modifier_item in modifier_items:
                            item_name = modifier_item.get('name')
                            # logging.info("parse item_name: " + product_name)
                            item_price = (modifier_item.get('priceV2').get('amountDisplay'))
                            result_item = {'product_id': product_id, 'category_name': category_name,
                                           'product_name': product_name, 'product_description': product_description,
                                           'product_price': product_price, 'product_image': product_image,
                                           'modifier_group': group_name, 'item_name': item_name,
                                           'item_price': item_price, 'required_or_not': group_required_or_not,
                                           'selection_type': group_select_type,
                                           'selection_range_min': group_min_available,
                                           'selection_range_max': group_max_available}
                            food_grab_list.append(result_item)
                            # logging.info(result_item)
                    else:
                        result['modifier_group'] = group_name
                        result['selection_type'] = group_select_type
                        result['required_or_not'] = group_required_or_not
                        result['selection_range_min'] = group_min_available
                        result['selection_range_max'] = group_max_available
                        item_price = modifier_group.get('priceV2').get('amountDisplay')
                        result['item_price'] = item_price
                        result['product_price'] = item_price
                        food_grab_list.append(result)
                        logging.info(result)

            else:
                food_grab_list.append(result)

    # logging.info(food_grab_list)
    food_grab_excel_list = []
    # assmbleExcel
    i = 0
    while i < len(food_grab_list):
        excel_language = variables.get('language', 'EN')
        excel_outlet_id = ''
        excel_outlet_services = ''
        excel_over_write = ''
        excel_category_name_en = (food_grab_list[i]).get('category_name') if isEn(variables) else ''
        excel_category_name_th = (food_grab_list[i]).get('category_name') if isTh(variables) else ''
        excel_category_name_cn = (food_grab_list[i]).get('category_name') if isCn(variables) else ''
        excel_category_sku = ''
        excel_category_description_en = ''
        excel_category_description_th = ''
        excel_category_description_cn = ''
        excel_item_name_en = (food_grab_list[i]).get('product_name') if isEn(variables) else ''
        excel_item_name_th = (food_grab_list[i]).get('product_name') if isTh(variables) else ''
        excel_item_name_cn = (food_grab_list[i]).get('product_name') if isCn(variables) else ''
        excel_item_sku = ''
        excel_item_image = (food_grab_list[i]).get('product_image')

        excel_description_en = (food_grab_list[i]).get('product_description') if isEn(variables) else ''
        excel_description_th = (food_grab_list[i]).get('product_description') if isTh(variables) else ''
        excel_description_cn = (food_grab_list[i]).get('product_description') if isCn(variables) else ''
        excel_conditional_modifier = ''
        excel_item_price = (food_grab_list[i]).get('item_price')
        excel_modifier_group_en = (food_grab_list[i]).get('modifier_group') if isEn(variables) else ''
        excel_modifier_group_th = (food_grab_list[i]).get('modifier_group') if isTh(variables) else ''
        excel_modifier_group_cn = (food_grab_list[i]).get('modifier_group') if isCn(variables) else ''
        excel_modifier_group_sku = ''
        excel_modifier_group_description_en = ''
        excel_modifier_group_description_th = ''
        excel_modifier_group_description_cn = ''

        excel_select_type = (food_grab_list[i]).get('selection_type')
        excel_required_or_not = (food_grab_list[i]).get('required_or_not')
        excel_min_available = (food_grab_list[i]).get('selection_range_min')
        excel_max_available = (food_grab_list[i]).get('selection_range_max')
        excel_modifier_en = (food_grab_list[i]).get('item_name') if isEn(variables) else ''
        excel_modifier_th = (food_grab_list[i]).get('item_name') if isTh(variables) else ''
        excel_modifier_cn = (food_grab_list[i]).get('item_name') if isCn(variables) else ''
        excel_modifier_sku = ''
        excel_modifier_description_en = ''
        excel_modifier_description_th = ''
        excel_modifier_description_cn = ''
        excel_options_price = (food_grab_list[i]).get('item_price')

        food_grab_excel_list.append(
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
    xlsx_path = os.path.join(homedir, "Aim_menu", "food_grab", f"{store_name}_{variables['language']}.xlsx")
    columns = ["Language", "Outlet ID", "Outlet services", "Overwrite (Y/N)", "category_name_en",
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
               "open_field2", "open_field3", "open_field4", "open_field5"]
    toExcel(columns, food_grab_excel_list, xlsx_path)
    print("Write file to " + xlsx_path)
    # print("Collection complete")
    return True

# if __name__ == '__main__':
#     url = 'https://food.grab.com/sg/en/restaurant/saap-saap-thai-jewel-changi-airport-b1-299-delivery/4-CZKELFETJBEBG6'
#     id = url.split('/')[-1]
#     parse_foodgrab(id, url)
