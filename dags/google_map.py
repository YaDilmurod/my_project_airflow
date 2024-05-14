def main():

    # %%
    from selenium import webdriver
    from bs4 import BeautifulSoup
    from selenium.webdriver.common.by import By
    from selenium.webdriver import ActionChains
    from selenium.webdriver.common.actions.wheel_input import ScrollOrigin
    import re
    import time
    import pandas as pd
    import numpy as np

    # %% [markdown]
    # ### Google Map Scrapping

    # %%
    filename = "data"
    search_queries = [
        "https://www.google.com/maps/search/%D1%88%D0%BA%D0%BE%D0%BB%D0%B0+%D0%B2+%D1%82%D0%B0%D1%88%D0%BA%D0%B5%D0%BD%D1%82%D0%B5/@41.3113397,69.251939,15z/data=!3m1!4b1?entry=ttu",
        "https://www.google.com/maps/search/maktab+%D0%B2+%D1%82%D0%B0%D1%88%D0%BA%D0%B5%D0%BD%D1%82/@41.3113025,69.2210391,13z/data=!3m1!4b1?entry=ttu",
        "https://www.google.com/maps/search/school+%D0%B2+%D1%82%D0%B0%D1%88%D0%BA%D0%B5%D0%BD%D1%82/@41.3112731,69.221039,13z/data=!3m1!4b1?entry=ttu",
        "https://www.google.com/maps/search/%D0%BF%D0%B0%D1%80%D0%BA%D0%B8+/@41.3113172,69.2210391,13z/data=!3m1!4b1?entry=ttu",
        "https://www.google.com/maps/search/korzinka/@41.3112584,69.221039,13z/data=!3m1!4b1?entry=ttu",
        "https://www.google.com/maps/search/havas/@41.3112438,69.221039,13z/data=!3m1!4b1?entry=ttu",
        "https://www.google.com/maps/search/baraka/@41.3112291,69.221039,13z/data=!3m1!4b1?entry=ttu",
        "https://www.google.com/maps/search/%D0%BF%D0%B0%D1%80%D0%BA+%D0%B2+%D1%82%D0%B0%D1%88%D0%BA%D0%B5%D0%BD%D1%82%D0%B5/@41.3300434,69.258444,12z/data=!3m1!4b1?entry=ttu",
        "https://www.google.com/maps/search/%D0%B1%D0%B0%D0%B7%D0%B0%D1%80/@41.3301021,69.2584441,12z/data=!3m1!4b1?entry=ttu",
        "https://www.google.com/maps/search/bolalar+bogchasi/@41.2960354,69.1294528,10.25z?entry=ttu",
        "https://www.google.com/maps/search/%D1%81%D1%82%D0%B0%D0%BD%D1%86%D0%B8%D1%8F+%D0%BC%D0%B5%D1%82%D1%80%D0%BE/@41.2935533,69.2620823,13.5z?entry=ttu",
        "https://www.google.com/maps/search/%D0%B4%D0%B5%D1%82%D1%81%D0%BA%D0%B8%D0%B9+%D1%81%D0%B0%D0%B4+%D0%B2+%D1%82%D0%B0%D1%88%D0%BA%D0%B5%D0%BD%D1%82%D0%B5/@41.2950951,69.129451,10z?entry=ttu"
    ]

    record = []
    e = []
    le = 0

    def google_map_extractor(query):
        link = f"{query}&hl=ru"
        browser = webdriver.Chrome()
        browser.get(link)
        time.sleep(5)
        action = ActionChains(browser)
        a = browser.find_elements(By.CLASS_NAME, "hfpxzc")
        
        while len(a) < 1000:
            var = len(a)
            scroll_origin = ScrollOrigin.from_element(a[0])
            action.scroll_from_origin(scroll_origin, 0, 20000).perform()
            time.sleep(3)
            a = browser.find_elements(By.CLASS_NAME, "hfpxzc")

            if len(a) == var:
                le+=1
                if le > 2:
                    break
            else:
                le = 0    


        browser.implicitly_wait(10)
        html_content = browser.page_source
        soup = BeautifulSoup(html_content, 'html.parser')
        div_elements = soup.find_all('div', class_=re.compile(r'^Nv2PK '))

        for div_element in div_elements:
            name = div_element.find('a', class_='hfpxzc')['aria-label']
            category_parent_divs = div_element.find_all('div', class_='W4Efsd')
            if len(category_parent_divs) >= 2:
                category_span =category_parent_divs[1].select('span > span')
                category = category_span[0].text.strip()
            else:
                category = ''
            href = div_element.find('a', class_='hfpxzc')['href']
            latitude = re.search(r'!3d(.*?)!4d(.*?)!', href).group(1)
            longitude = re.search(r'!3d(.*?)!4d(.*?)!', href).group(2)
            if name not in record:
                record.append((name,category,latitude,longitude))
    

    for query in search_queries:
        print(query)
        google_map_extractor(query)


    df = pd.DataFrame(record, columns=['name', 'category', 'lat', 'long'])
    file_name = '../Data/Map_Data.xlsx'
    df.to_excel(file_name, index=False)

    # %%
    df = pd.read_excel('/Users/didi/Desktop/data_scrapping/Data Scrapping/Data/Map_Data.xlsx')
    categories_to_keep = ['Школа', 'Супермаркет', 'Магазин', 'Частная школа', 'Средняя школа', 'Начальная школа',
                        'Международная школа', 'Торговый центр', 'Продовольственный магазин',
                        'Магазин шаговой доступности', 'Супермаркет низких цен', 'Ресторан', 'фастфуд',
                        'Узбекская кухня','Кафе','Суши','Турецкая кухня','Гамбургеры','Корейская кухня','Японская кухня','Еда на вынос','Доставка готовой еды','Парк',  'Станция метро',  'Детский сад']

    df = df[df['category'].isin(categories_to_keep)]

    corrections = {
        'Магазин': 'grocery',
        'Супермаркет': 'grocery',
        'Продовольственный магазин': 'grocery',
        'Магазин шаговой доступности': 'grocery',
        'Супермаркет низких цен': 'grocery',
        'Рынок': 'grocery',
        'Начальная школа': 'school',
        'Частная школа': 'school',
        'Средняя школа': 'school',
        'Школа': 'school',
        'Международная школа': 'school',
        'Торговый центр': 'mall',
        'Парк': 'park',
        'Парк': 'park',
        'Станция метро': 'metro_station',
        'Детский сад': 'kindergarten'
    }

    df['category'] = df['category'].replace(corrections)

    df.dropna(subset=['category', 'lat', 'long'], inplace=True)

    df.drop_duplicates(subset=['category', 'lat', 'long'], inplace=True)

    # %% [markdown]
    # ### School and Kindergarten

    # %%
    not_school_df = df[(df['category'] != 'school') & (df['category'] != 'kindergarten')].copy()
    not_school_df['name'] = not_school_df['name'].str.lower()
    school_with_numbers_df = df[(df['category'] == 'school') & df['name'].str.contains('\d')].copy()
    school_with_numbers_df['name'] = school_with_numbers_df['name'].str.extract('(\d+)')

    kindergartens_with_numbers_df = df[(df['category'] == 'kindergarten') & df['name'].str.contains('\d')].copy()
    kindergartens_with_numbers_df['name'] = kindergartens_with_numbers_df['name'].str.extract('(\d+)')

    df = pd.concat([not_school_df, school_with_numbers_df, kindergartens_with_numbers_df])

    # %% [markdown]
    # ### Grogeries

    # %%
    conditions = [
        (df['category'] == 'grocery') & df['name'].str.lower().str.contains('havas', case=False, na=False),
        (df['category'] == 'grocery') & df['name'].str.lower().str.contains('korzinka', case=False, na=False),
        (df['category'] == 'grocery') & df['name'].str.lower().str.contains('baraka', case=False, na=False),
        (df['category'] == 'grocery') & ~(df['name'].str.lower().str.contains('havas', case=False, na=False) |
        df['name'].str.lower().str.contains('korzinka', case=False, na=False) |
        df['name'].str.lower().str.contains('baraka', case=False, na=False)),
    ]
    choices = ['havas', 'korzinka', 'baraka', None]
    df['name'] = np.select(conditions, choices, default=df['name'])
    df = df[~df['name'].isna()]

    # %% [markdown]
    # ### Parks

    # %%
    contains_park = df['name'].str.lower().str.contains('парк', na=False)
    df.loc[contains_park, 'name'] = df.loc[contains_park, 'name'].str.replace('парк', '', case=False)
    df['name'] = df['name'].str.strip()

    # %%
    df.dropna(subset=['name'], inplace=True)
    df = df[df['name'] != '']
    df.drop_duplicates(subset=['name', 'category', 'lat', 'long'], inplace=True)

    # %%
    file_name = '../Data/Cleaned_Map_Data.xlsx'
    df.to_excel(file_name, index=False)


