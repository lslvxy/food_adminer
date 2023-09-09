import json
import logging
import os
import re
from datetime import datetime

import pandas as pd
import requests
import time
from bs4 import BeautifulSoup

from utils import isEn, init_path
from utils import isCn
from utils import isTh
from utils import fixStr
from utils import toExcel

import pathlib

bc = logging.basicConfig(level=logging.INFO, format='%(asctime)s  - %(message)s')


class FoodGrabItem:
    spider_type = 'food_grab'
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
    store_data = fetch_data(page_url, variables)
    item = FoodGrabItem()
    item.id = store_data.get('ID')
    item.store_name = store_data.get('name')
    item.photoHref = store_data.get('photoHref')
    category_list = store_data.get('menu').get('categories')
    item.menus = category_list
    item.language = variables['language']
    compose_images(item)
    # save_images(item)
    process_category(item)
    process_product(item)
    process_group(item)
    process_item(item)
    process_final_list(item)
    process_excel(item)


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
    all_dir_path = init_path('food_grab', item.store_name, "ALL")
    for ca in item.menus:
        category_name = f"{fixStr(ca['name'])}"
        category_dir_path = init_path('food_grab', item.store_name, category_name)
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
    for category in item.menus:
        category_name = f"{fixStr(category['name'])}"
        category_data = {
            'category_id': '',
            'category_name': category_name,
            'category_description': '',
            'category_image': ''
        }
        total_category_list.append(category_data)
    item.total_category_list = total_category_list


def process_product(item):
    total_product_map = {}
    for category in item.menus:
        category_name = f"{fixStr(category['name'])}"
        source_product_list = category.get('items')
        if source_product_list is None:
            continue
        for p_idx, product in enumerate(source_product_list):
            product_id = product.get('ID')
            if not product_id:
                timestamp = datetime.timestamp(datetime.now())
                product_id = f'SI{p_idx}{timestamp}'
                product['ID'] = product_id
            product_name = fixStr(product.get('name'))
            product_description = product.get('description')
            product_price = product.get('discountedPriceV2').get('amountDisplay')
            product_image = fixStr(product['name']) + '.jpg'
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
                all_sub_group_id = []
                for modifier_group in modifier_groups:
                    all_sub_group_id.append(modifier_group.get('ID'))
                product_data['sub_product_ids'] = '|'.join(all_sub_group_id)

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
            group_id = modifier_group.get('ID')
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
                all_sub_modifiers_id = []
                for modifier in modifiers:
                    all_sub_modifiers_id.append(modifier.get('ID'))
                group_data['sub_product_ids'] = '|'.join(all_sub_modifiers_id)

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
            modifier_id = modifier_item.get('ID')
            modifier_name = modifier_item.get('name')

            item_price = modifier_item.get('priceV2').get('amountDisplay')
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
        category_name = data.get('category_name')
        if not sub_product_ids:
            continue
        for _old, _new in delete_ids.items():
            new_category_name = _list.get(_new).get('category_name')
            if category_name and new_category_name not in category_name:
                data['category_name'] = '|'.join([category_name, new_category_name])
            if _old in sub_product_ids.split('|'):
                sub_product_ids = sub_product_ids.replace(_old, _new)
        data['sub_product_ids'] = sub_product_ids
        _list[p_id] = data
    return _list


def build_key(data):
    key = '_'.join([data.get('product_type'), data.get('product_name'),
                    data.get('sub_product_ids'),
                    str(data.get('product_price', '')), str(data.get('selection_range_min', '')),
                    str(data.get('selection_range_max', ''))])
    return key


def process_final_list(item):
    total_product_map = item.total_product_map
    total_group_map = item.total_group_map
    total_modifier_map = item.total_modifier_map
    delete_ids = {}
    all_data_product = dict(**dict(**total_product_map, **total_group_map), **total_modifier_map)
    for i in range(3):
        all_data = {}
        for productId, _data in all_data_product.items():
            key = build_key(_data)
            if key in all_data:
                ori_product_id = all_data.get(key)
                delete_ids[productId] = ori_product_id
            else:
                all_data[key] = productId
        all_data_product = replace_sub_id(all_data_product, delete_ids)

    if delete_ids:
        for _id in delete_ids.keys():
            if _id in all_data_product:
                del all_data_product[_id]
    item.fina_data = all_data_product.values()


def process_excel(item):
    homedir = str(pathlib.Path.home())
    all_data_product = item.fina_data
    total_category_list = item.total_category_list

    all_excel_data_product = []
    all_excel_data_category = []
    for dd in all_data_product:
        all_excel_data_product.append(
            [dd.get('id'), dd.get('product_id'), dd.get('product_type'), dd.get('product_name'),
             dd.get('category_name'), dd.get('sub_product_ids'), dd.get('product_description'),
             dd.get('product_price'), dd.get('selection_range_min'), dd.get('selection_range_max'),
             dd.get('product_image'), dd.get('blockList')])
    for cc in total_category_list:
        all_excel_data_category.append(
            [cc.get('category_id'), cc.get('category_name'), cc.get('category_description'),
             cc.get('category_image')])

    columns_sheet_category = [("categoryId", "description", "demo"), ("categoryName", "description", "demo"),
                              ("description_Default", "description", "demo"),
                              ("categoryImage", "description", "demo")]

    columns_sheet_product = [("productId", "description", "demo"), ("posProductId", "description", "demo"),
                             ("productType", "description", "demo"), ("name", "description", "demo"),
                             ("category", "description", "demo"), ("subPosProductIds", "description", "demo"), (
                                 "description", "description", "demo"), (
                                 "price", "description", "demo"), ("min", "description", "demo"),
                             ("max", "description", "demo"), ("images", "description", "demo"),
                             ("blockList", "description", "demo")]

    xlsx_path = os.path.join(homedir, "Aim_menu", "food_grab",
                             f"{item.store_name}_{item.language}_V2.xlsx")
    # toExcel(columns, all_excel_data, xlsx_path)
    df1 = pd.DataFrame(all_excel_data_category, columns=pd.MultiIndex.from_tuples(columns_sheet_category))
    df1.index = range(1, len(df1) + 1)

    df2 = pd.DataFrame(all_excel_data_product, columns=pd.MultiIndex.from_tuples(columns_sheet_product))
    df2.index = range(1, len(df2) + 1)
    with pd.ExcelWriter(xlsx_path) as writer:

        df1.to_excel(writer, sheet_name='categoryList')
        df2.to_excel(writer, sheet_name='productList')
        writer.sheets['categoryList'].delete_rows(4)
        writer.sheets['productList'].delete_rows(4)

    print("Collection complete")

# if __name__ == '__main__':
#     url = 'https://food.grab.com/sg/en/restaurant/saap-saap-thai-jewel-changi-airport-b1-299-delivery/4-CZKELFETJBEBG6'
#     id = url.split('/')[-1]
#     parse_foodgrab(id, url)
