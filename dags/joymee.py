def main():

    # %%
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.common.exceptions import NoSuchElementException, TimeoutException
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    import time
    from datetime import datetime

    options = Options()
    driver = webdriver.Chrome(options=options)
    url = 'https://joymee.uz/ru/tashkent/prodazha'
    driver.get(url)
    wait = WebDriverWait(driver, 5)

    while True:
        try:
            
            load_button = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, "action-load-span-end")))
            # load_button.click()

            # # Additional waiting for any dynamic changes or updates
            # wait.until(EC.staleness_of(load_button))
            driver.execute_script("arguments[0].click();", load_button)


            # btn = WebDriverWait(driver, 2).until(  
            #     EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, 'width250')]"))
            # )
            # btn.click()
            time.sleep(2)
            # if(len(driver.find_elements(By.XPATH, "//button[contains(@class, 'width250')]")) >=1):
            #     break
            # driver.execute_script("location.reload(true);")
        except NoSuchElementException:
            print('NoSuchElement')
            break
        except TimeoutException:
            print('Timeout')
            break  # Add this line to break out of the loop when TimeoutException occurs

    doc = driver.page_source
    driver.close()

    # %%
    from bs4 import BeautifulSoup
    import requests
    import pandas as pd

    def extract_currency_and_value(html_element):
        if html_element:
            parts = html_element.text.strip().split(' у.е.')
            currency = 'None'
            price = 'None'

            if len(parts) >= 2:
                price = parts[-2].strip()
                currency = 'USD'

            return price, currency
        else:
            return 'None', 'None'
        
    def extract_lat_long(html_element):
        latitude = 'None'
        longitude = 'None'

        if html_element:
            # Assuming latitude and longitude are present in data attributes
            data_coordinates = html_element.get('data-coordinates')
            if data_coordinates:
                latitude, longitude = map(float, data_coordinates.split(','))

        return latitude, longitude


    final_data = []

    # Parse 'doc' with BeautifulSoup
    soup = BeautifulSoup(doc, 'html.parser')

    # Find all links inside the specified div
    apartment_links = soup.select('div.catalog-results div.item-grid-img a')

    # Loop through each link
    for apartment_link in apartment_links:
        link = apartment_link['href']
        
        # Print the link before making the request
        # print(f"Processing link: {link}")

        try:
            # Get the HTML content of the linked page
            linked_page_content = requests.get(link).text

            # Parse the HTML content with BeautifulSoup
            linked_soup = BeautifulSoup(linked_page_content, 'html.parser')

            # Extract logical elements and store in a dictionary
            data = {}
            for item in linked_soup.select('.list-properties-item'):
                key = item.select_one('.list-properties-span1').text.strip()
                value = item.select_one('.list-properties-span2').text.strip()
                data[key] = value


            # Extract and append price and currency to the data dictionary
            price_element = linked_soup.select_one('.board-view-price.price-currency')
            price, currency = extract_currency_and_value(price_element)
            data['Цена'] = price
            data['Валюта'] = currency

            map_element = linked_soup.select_one('.ads-view-map')

            description_element = linked_soup.select_one('div.word-break')
            description = description_element.text.strip() if description_element else 'None'
            data['Описание'] = description

            description_element = linked_soup.select_one('.h1title')
            description = description_element.text.strip() if description_element else 'None'
            data['Название'] = description

            # Append data to the final_data list
            final_data.append(data)
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data from {link}: {e}")


    # %%
    # Create a DataFrame from the list of dictionaries
    final_df = pd.DataFrame(final_data)

    # %%
    final_df['Количество комнат'] = pd.to_numeric(final_df['Количество комнат'].str.extract('(\\d+)', expand=False), errors='coerce')
    final_df['Цена'] = pd.to_numeric(final_df['Цена'].str.replace(',', ''), errors='coerce')

    # %%
    from bs4 import BeautifulSoup
    import pandas as pd
    from urllib.parse import urlparse, parse_qs
    from datetime import datetime, timedelta
    from dateutil import parser as date_parser

    def parse_custom_date(raw_date):
        # Map Russian month names to their numerical representations
        month_mapping = {
            'января': 1, 'февраля': 2, 'марта': 3,
            'апреля': 4, 'мая': 5, 'июня': 6,
            'июля': 7, 'августа': 8, 'сентября': 9,
            'октября': 10, 'ноября': 11, 'декабря': 12
        }

        if "сегодня" in raw_date:
            return datetime.now().strftime('%d.%m.%Y')
        elif "вчера" in raw_date:
            yesterday = datetime.now() - timedelta(days=1)
            return yesterday.strftime('%d.%m.%Y')
        else:
            # Replace Russian month names with numerical representations
            for month_name_ru, month_number in month_mapping.items():
                raw_date = raw_date.replace(month_name_ru, str(month_number))

            # Parse the date
            parsed_date = date_parser.parse(raw_date, dayfirst=True) if raw_date else None
            return parsed_date.strftime('%d.%m.%Y') if parsed_date else None

    location_links = BeautifulSoup(doc, 'html.parser').find_all('div', class_='ads_contact')
    date_elements = BeautifulSoup(doc, 'html.parser').find_all('span', class_='item-grid-date')

    links = [a['href'] for div in location_links for a in div.find_all('a', class_='btn-color-purple')]

    # Create empty lists to store latitude, longitude, and date
    latitudes = []
    longitudes = []
    dates = []

    # Parse each URL and extract latitude, longitude, and date
    for link, date_element in zip(links, date_elements):
        parsed_url = urlparse(link)
        query_parameters = parse_qs(parsed_url.query)
        
        # Extract latitude and longitude
        latitude = query_parameters.get('query')[0].split(',')[0]
        longitude = query_parameters.get('query')[0].split(',')[1]
        
        # Extract date and parse it into a consistent format
        raw_date = date_element.text.strip() if date_element else None
        formatted_date = parse_custom_date(raw_date)
        
        # Convert to float if needed
        latitude = float(latitude)
        longitude = float(longitude)
        
        # Append to the lists
        latitudes.append(latitude)
        longitudes.append(longitude)
        dates.append(formatted_date)

    # Create a DataFrame
    data = {'Широта': latitudes, 'Долгота': longitudes, 'Дата публикации': dates}
    location_df = pd.DataFrame(data)

    # %%
    location_df = location_df[['Широта', 'Долгота', 'Дата публикации']]
    final_df = final_df.merge(location_df, left_index=True, right_index=True)

    final_df.loc[final_df['Площадь, м²'].notnull(), 'Тип'] = 'Квартира'
    final_df.loc[final_df['Площадь, м²'].isnull(), 'Тип'] = 'Участок'
    final_df['Площадь, м²'] = final_df['Площадь, м²'].fillna(final_df['Площадь соток'].astype(float) * 100)

    # %%
    column_name_mapping = {
        "Тип квартиры": "Тип постройки",
        "Этажность дома": "Этажность",
        "Площадь, м²": "Площадь", 
    }

    # Rename the columns
    final_df.rename(columns=column_name_mapping, inplace=True)
    final_df.head()

    # %%
    columns_to_check = ["Источник", "Название", "Тип","Санузел", "Тип постройки", "Материал", "Широта", 
                        "Долгота", "Район", "Этаж", "Этажность", "Ремонт", "Площадь", 
                        "Количество комнат", "Дата публикации", "Валюта", "Цена", "Дата создания"]
    # Create a new DataFrame with the specified columns
    new_df = pd.DataFrame(columns=columns_to_check)

    # Check if columns exist in final_df and create them with None values if not
    for column in columns_to_check:
        if column not in final_df.columns:
            final_df[column] = None
            new_df[column] = None
        else:
            new_df[column] = final_df[column]
            
    new_df["Источник"] = 'Joymee'
    new_df["Район"] = ''
    new_df["Дата создания"] = datetime.now().strftime("%d.%m.%Y")
    new_df[columns_to_check]
    new_df.head()


    # %%
    # Specify columns to check for duplicates
    columns_to_check_dup = ["Источник", "Название", "Тип", "Санузел", "Тип постройки", "Материал", 
                        "Широта", "Долгота", "Район", "Этаж", "Этажность", "Ремонт", "Площадь", 
                        "Количество комнат", "Дата публикации", "Валюта", "Цена"]

    # Count the number of rows before removing duplicates
    rows_before = new_df.shape[0]

    # Remove duplicates based on specified columns
    df_no_duplicates = new_df.drop_duplicates(subset=columns_to_check_dup, keep=False)

    # Count the number of rows after removing duplicates
    rows_after = df_no_duplicates.shape[0]

    # Calculate the number of rows deleted
    rows_deleted = rows_before - rows_after

    print(f"\nNumber of rows deleted: {rows_deleted}")

    # %%
    import pandas as pd
    import os

    # Assuming x is your variable and data is the data you want to store
    name_of_file = "Joymee"
    df = pd.DataFrame(df_no_duplicates)

    # Set the path to the Excels folder (assuming it is a sibling of the Notebooks folder)
    excels_folder_path = os.path.join(os.path.dirname(os.getcwd()), "Excels")

    # Check if the folder exists, if not, create it
    if not os.path.exists(excels_folder_path):
        os.makedirs(excels_folder_path)

    # Create a folder with the name_of_file only if it doesn't exist
    file_folder_path = os.path.join(excels_folder_path, name_of_file)

    if not os.path.exists(file_folder_path):
        os.makedirs(file_folder_path)

    excel_file_name = os.path.join(file_folder_path, f"{name_of_file}.xlsx")

    # Check if the file already exists
    if os.path.exists(excel_file_name):
        # Read the existing Excel file into a DataFrame
        existing_df = pd.read_excel(excel_file_name)

        # Append the new data to the existing DataFrame
        updated_df = pd.concat([existing_df, df], ignore_index=True)

        # Check for duplicates in all columns
        duplicates_mask = updated_df.duplicated(keep=False)

        # Print the number of duplicates
        num_duplicates = duplicates_mask.sum()
        print(f"Number of duplicates after adding new data: {num_duplicates}")

        # If duplicates exist, remove them
        if any(duplicates_mask):
            updated_df = updated_df[~duplicates_mask]

        # Write the updated DataFrame back to the Excel file
        updated_df.to_excel(excel_file_name, index=False)

        print(f"Data added to existing Excel file '{excel_file_name}' after removing duplicates.")
    else:
        # If the file doesn't exist, create a new Excel file with the data
        df.to_excel(excel_file_name, index=False)
        print(f"Excel file '{excel_file_name}' created with new data.")



