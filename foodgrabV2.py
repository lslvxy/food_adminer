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
    save_images(item)
    process_category(item)
    process_product(item)
    process_group(item)
    process_item(item)
    process_final_list(item)
    process_excel(item)

    parse_foodgrabV1(item, variables)


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

    all_excel_data_product = [['demo', '必填', '必填\n校验是否属于 MODIFIER / SINGLE / GROUP', '必填\n校验字符长度 (64)',
                              '必填\n校验名称是否能对应categoryList的记录',
                              '非必填', '非必填\n如有填，校验字符长度 (128)',
                              '必填\n校验是否是数字\n最高数9999999999.99',
                              '必填\n校验是否是数字', '必填\n校验是否是数字\n校验数字是否大于min',
                              '非必填\n校验图片是否已上传', '非必填']]
    all_excel_data_category = [['demo', '必填\n校验字符长度 (64)\n校验是否有重名', '非必填\n如有填，校验字符长度 (128)',
                               '非必填\n校验图片是否已上传']]
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

    columns_sheet_category = ["categoryID", "categoryName(Optional)", "categoryDescription(Optional)", "categoryimage(Optional)"]

    columns_sheet_product = ["productId", "posProductId", "productType", "name", "category",
                             "subPosProductIds", "description", "price", "min", "max", "images",
                             "blockList"]

    xlsx_path = os.path.join(homedir, "Aim_menu", "food_grab",
                             f"{item.store_name}_{item.language}_V2.xlsx")
    # toExcel(columns, all_excel_data, xlsx_path)
    df1 = pd.DataFrame(all_excel_data_category, columns=columns_sheet_category)
    # df1.index = range(1, len(df1) + 1)

    df2 = pd.DataFrame(all_excel_data_product, columns=columns_sheet_product)
    # df2.index = range(1, len(df2) + 1)
    with pd.ExcelWriter(xlsx_path) as writer:

        df1.to_excel(writer, sheet_name='categoryList', index=False)
        df2.to_excel(writer, sheet_name='productList', index=False)
        # writer.sheets['categoryList'].delete_rows(4)
        # writer.sheets['productList'].delete_rows(4)

    print("Collection complete")


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
            logging.info("parse product_name: " + product_name)
            product_description = product.get('description')
            product_price = product.get('discountedPriceV2').get('amountDisplay')
            product_image = fixStr(product_name) + '.jpg'
            result = {'product_id': product_id, 'category_name': category_name,
                      'product_name': product_name, 'product_description': product_description,
                      'product_price': product_price, 'product_image': product_image}

            modifier_groups = product.get('modifierGroups')
            if modifier_groups:
                for modifier_group in modifier_groups:
                    group_name = modifier_group.get('name')
                    logging.info("parse group_name: " + product_name)
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
                            logging.info("parse item_name: " + product_name)
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
                            logging.info(result_item)
                    else:
                        result['modifier_group'] = group_name
                        result['selection_type'] = group_select_type
                        result['required_or_not'] = group_required_or_not
                        result['selection_range_min'] = group_min_available
                        result['selection_range_max'] = group_max_available
                        item_price = modifier_group.get('discountedPriceV2').get('amountDisplay')
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
    print("Collection complete")
    return True

# if __name__ == '__main__':
#     url = 'https://food.grab.com/sg/en/restaurant/saap-saap-thai-jewel-changi-airport-b1-299-delivery/4-CZKELFETJBEBG6'
#     id = url.split('/')[-1]
#     parse_foodgrab(id, url)
