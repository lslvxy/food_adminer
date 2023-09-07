import json
import logging
import os
import re
import pandas as pd
import requests
import time
from bs4 import BeautifulSoup

from utils import isEn
from utils import isCn
from utils import isTh
import pathlib

bc = logging.basicConfig(level=logging.INFO, format='%(asctime)s  - %(message)s')


def fetch_soup(url):
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
    soup = BeautifulSoup(response.text, 'html.parser')
    return soup


def find_by_name(categories, name):
    for c in categories:
        if c.get('name') == name:
            return c


def fetch_restaurant(page_url):
    soup = fetch_soup(page_url)
    if soup is None:
        return None, None

    category_soup = soup.find_all("script", class_="next-head")
    i = 0
    while i < 10:
        if category_soup:
            print("Data has been pulled and is being parsed")
            break
        i += 1
        print("No data pulled, retrying")
        time.sleep(5)
        soup = fetch_soup(page_url)
        category_soup = soup.find_all("script", class_="next-head")
    if i == 10 and category_soup == []:
        print("Failed to pull data, please check network link and try again later")
        return None, None

    data = {}
    for category in category_soup:
        cat_json = json.loads(category.get_text())
        if cat_json['@type'] == 'Restaurant':
            data = cat_json
            break
    return data, soup


def fetch_config_data(soup, variables):
    next_data = json.loads(soup.find("script", id="__NEXT_DATA__").get_text())
    product_options_list = next_data.get('props').get('initialReduxState').get('pageRestaurantDetail').get(
        'entities').get(variables['id']).get('menu').get('categories')
    return product_options_list


# def output_image():

def parse_foodgrab(page_url, variables):
    homedir = str(pathlib.Path.home())
    restaurant_data, soup = fetch_restaurant(page_url)
    if restaurant_data is None:
        return None
    product_options_list = fetch_config_data(soup, variables)
    title = soup.find("title").get_text()
    if ':' in title:
        store_name = title.split(":")[0]
    else:
        store_name = title
    # print(menu_categories)
    tab_categories = restaurant_data.get('hasMenu').get('hasMenuSection')
    food_grab_list = []
    for category in tab_categories:
        category_name = re.sub(r'[:/\\?*“”<>|""]', '_', category.get('name'))
        product_option = find_by_name(product_options_list, category.get('name'))
        products = category.get('hasMenuItem')
        # if len(products) == 1:
        #     logging.info("only one product")
        # elif len(products) > 1:
        #     logging.info("multiple products")
        #     for pv in products:
        #         result = {'category': category_name, 'category_description': '',
        #                   'item_name': '', 'description': '', 'package_type': 'Combo',
        #                   'package_price': pv.get('offers').get('price'), 'options': pv.get('name', '')}
        #         food_grab_list.append(result)

        for product in products:
            out = {}
            product_name = product.get('name')
            item_name = re.sub(r'[:/\\?*“”<>|""]', '_', product_name)
            # out.clear()
            total_category = category_name
            # os.path.join(homedir, "Aim_menu", "food_grab", f"{store_name}", f"{category_name}")
            dirPath = os.path.join(homedir, "Aim_menu", "food_grab", f"{store_name.strip()}",
                                   f"{category_name.strip()}")
            if not os.path.exists(dirPath):
                os.makedirs(dirPath)
            total_item_name = item_name
            total_description = product.get('description')
            total_item_price = product.get('offers').get('price')
            total_item_image = ''
            # out['币种'] = str(product.get('offers').get('priceCurrency'))
            # food_grab_list.append(out)
            out = {}
            out['category'] = total_category
            out['item_name'] = total_item_name
            out['description'] = total_description
            out['item_price'] = total_item_price
            # out['modifier_group'] = total_modifier_group
            # out['select_type'] = total_select_type
            # out['required_or_not'] = total_required_or_not
            # out['min_available'] = total_min_available
            # out['max_available'] = total_max_available
            # out['options'] = total_options
            # out['options_price'] = total_options_price
            if product_option is not None:
                product_item = find_by_name(product_option['items'], product_name)
                product_item_image = product_item.get('images')
                if product_item_image:
                    item_image = product_item.get('images')[0]
                    r = requests.get(item_image, timeout=180)
                    image_path = os.path.join(dirPath, f"{item_name.strip()}.jpg")
                    with open(image_path, 'wb') as f:
                        f.write(r.content)
                    print("Download image: " + image_path)
                    out['item_image'] = f"{item_name.strip()}.jpg"
                    total_item_image = f"{item_name.strip()}.jpg"
                else:
                    out['item_image'] = ''

                modifier_groups = product_item.get('modifierGroups')
                if modifier_groups:
                    for modifier_group in modifier_groups:
                        total_modifier_group = modifier_group.get('name')
                        total_select_type = 'Single' if modifier_group.get(
                            'selectionType') == 0 else 'Multiple'
                        if modifier_group.get('selectionRangeMin') == modifier_group.get(
                                'selectionRangeMax') == 1:
                            total_required_or_not = "TRUE"
                        else:
                            total_required_or_not = "FALSE"

                        total_min_available = modifier_group.get('selectionRangeMin')
                        total_max_available = modifier_group.get('selectionRangeMax')
                        modifier_items = modifier_group.get('modifiers')
                        for modifier_item in modifier_items:
                            total_options = modifier_item.get('name')
                            total_options_price = (modifier_item.get('priceV2').get('amountDisplay'))
                            out = {}
                            out['category'] = total_category
                            out['item_name'] = total_item_name
                            out['item_image'] = total_item_image
                            out['description'] = total_description
                            out['item_price'] = total_item_price
                            out['modifier_group'] = total_modifier_group
                            out['select_type'] = total_select_type
                            out['required_or_not'] = total_required_or_not
                            out['min_available'] = total_min_available
                            out['max_available'] = total_max_available
                            out['options'] = total_options
                            out['options_price'] = total_options_price
                            food_grab_list.append(out)
                else:
                    food_grab_list.append(out)
            else:
                food_grab_list.append(out)

    food_grab_excel_list = []
    i = 0
    while i < len(food_grab_list):
        excel_language = variables.get('language', 'EN')
        excel_outlet_id = ''
        excel_outlet_services = ''
        excel_over_write = ''
        excel_category_name_en = (food_grab_list[i]).get('category') if isEn(variables) else ''
        excel_category_name_th = (food_grab_list[i]).get('category') if isTh(variables) else ''
        excel_category_name_cn = (food_grab_list[i]).get('category') if isCn(variables) else ''
        excel_category_sku = ''
        excel_category_description_en = ''
        excel_category_description_th = ''
        excel_category_description_cn = ''
        excel_item_name_en = (food_grab_list[i]).get('item_name') if isEn(variables) else ''
        excel_item_name_th = (food_grab_list[i]).get('item_name') if isTh(variables) else ''
        excel_item_name_cn = (food_grab_list[i]).get('item_name') if isCn(variables) else ''
        excel_item_sku = ''
        excel_item_image = (food_grab_list[i]).get('item_image')

        excel_description_en = (food_grab_list[i]).get('description') if isEn(variables) else ''
        excel_description_th = (food_grab_list[i]).get('description') if isTh(variables) else ''
        excel_description_cn = (food_grab_list[i]).get('description') if isCn(variables) else ''
        excel_conditional_modifier = (food_grab_list[i]).get('package_type')
        excel_item_price = (food_grab_list[i]).get('item_price')
        excel_modifier_group_en = (food_grab_list[i]).get('modifier_group') if isEn(variables) else ''
        excel_modifier_group_th = (food_grab_list[i]).get('modifier_group') if isTh(variables) else ''
        excel_modifier_group_cn = (food_grab_list[i]).get('modifier_group') if isCn(variables) else ''
        excel_modifier_group_sku = ''
        excel_modifier_group_description_en = ''
        excel_modifier_group_description_th = ''
        excel_modifier_group_description_cn = ''

        excel_select_type = (food_grab_list[i]).get('select_type')
        excel_required_or_not = (food_grab_list[i]).get('required_or_not')
        excel_min_available = (food_grab_list[i]).get('min_available')
        excel_max_available = (food_grab_list[i]).get('max_available')
        excel_modifier_en = (food_grab_list[i]).get('options') if isEn(variables) else ''
        excel_modifier_th = (food_grab_list[i]).get('options') if isTh(variables) else ''
        excel_modifier_cn = (food_grab_list[i]).get('options') if isCn(variables) else ''
        excel_modifier_sku = ''
        excel_modifier_description_en = ''
        excel_modifier_description_th = ''
        excel_modifier_description_cn = ''
        excel_options_price = (food_grab_list[i]).get('options_price')

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
    df = pd.DataFrame(food_grab_excel_list,
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
    xlsx_path = os.path.join(homedir, "Aim_menu", "food_grab", f"{store_name}_{variables['language']}.xlsx")
    if os.path.exists(xlsx_path):
        os.remove(xlsx_path)
    print("Write file to " + xlsx_path)
    df.to_excel(xlsx_path, index=False)
    print("Collection complete")
    return True

# if __name__ == '__main__':
#     url = 'https://food.grab.com/sg/en/restaurant/saap-saap-thai-jewel-changi-airport-b1-299-delivery/4-CZKELFETJBEBG6'
#     id = url.split('/')[-1]
#     parse_foodgrab(id, url)

