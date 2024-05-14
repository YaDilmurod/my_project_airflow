def main():
    # %%
    import requests
    from bs4 import BeautifulSoup
    from datetime import datetime
    import pandas as pd

    # %%
    def extract_currency_and_value(column_value):
        parts = column_value.split(' у.е.')
        currency = 'None'
        price = 'None'

        if len(parts) >= 2:
            price = parts[-2].strip()
            currency = 'USD'

        return price, currency

    def extract_date(date_element):
        date_str = date_element.text.strip()
        date_format = "%d.%m.%Y %H:%M:%S"
        date_obj = datetime.strptime(date_str, date_format)
        return date_obj

    def scrape_dom_uz_detail(detail_url):
        result_entry = {}

        try:
            response = requests.get(detail_url)

            if response.status_code == 200:
                apartment_soup = BeautifulSoup(response.text, 'html.parser')

                # Extract the information from the detailed page
                title_element = apartment_soup.find('div', class_='page-title')  # Adjust the class based on the website structure
                date_element = apartment_soup.find('div', class_='date')

                if title_element and date_element:
                    apartment_title = title_element.text.strip()
                    apartment_date = extract_date(date_element)

                    # Extract information from ul element with class table-block
                    details_element = apartment_soup.find('ul', class_='table-block')
                    details = {}

                    if details_element:
                        for strong_tag, span_tag in zip(details_element.find_all('strong'), details_element.find_all('span')):
                            column_name = strong_tag.text.strip()[:-1]
                            column_value = span_tag.text.strip() if span_tag else 'None'

                            # Special handling for the "Цена" column
                            if column_name == 'Цена':
                                price, currency = extract_currency_and_value(column_value)
                                details['Валюта'] = currency
                                details['Цена'] = price
                            else:
                                details[column_name] = column_value

                    # Extract latitude and longitude
                    lon_input = apartment_soup.find('input', {'name': 'YMAP_POINT_LON'})
                    lat_input = apartment_soup.find('input', {'name': 'YMAP_POINT_LAT'})
                    longitude = lon_input['value'] if lon_input else 'None'
                    latitude = lat_input['value'] if lat_input else 'None'
                    details['Долгота'] = longitude
                    details['Широта'] = latitude

                    # Add the information to the result entry
                    result_entry = {
                        'Название': apartment_title,
                        'Дата': apartment_date,
                        **details
                    }

        except Exception as e:
            print(f"An exception occurred while scraping {detail_url}: {e}")

        return result_entry

    def scrape_dom_uz_range(main_url, start_page, stop_page):
        results = []

        for page in range(start_page, stop_page + 1):
            detail_url = f"{main_url}detail/{page}"
            print(detail_url)
            result_entry = scrape_dom_uz_detail(detail_url)

            if result_entry:
                results.append(result_entry)

        # Create a DataFrame from the results list
        final_df = pd.DataFrame(results)

        return final_df

    # Specify the range of page numbers you want to scrape
    start_page_number = 13500
    stop_page_number = start_page_number + 1000

    main_url_dom_uz = "https://dom.uz/catalog/"
    final_df = scrape_dom_uz_range(main_url_dom_uz, start_page_number, stop_page_number)

    # %%
    final_df = final_df.rename(columns={'Массив, улица': 'Адрес', 'Вид материала': 'Материал'})
    final_df['Площадь'] = final_df['Общая площадь, м2'].fillna(final_df['Площадь дома'])
    final_df['Цена'] = pd.to_numeric(final_df['Цена'].str.replace(' ', ''), errors='coerce')
    final_df['Количество комнат'] = pd.to_numeric(final_df['Количество комнат'].str.extract('(\\d+)', expand=False), errors='coerce')
    final_df[['Долгота', 'Широта']] = final_df[['Долгота', 'Широта']].apply(pd.to_numeric, errors='coerce')

    numeric_columns = ['Этаж квартиры', 'Этажность дома', 'Площадь', 'Количество соток']
    final_df[numeric_columns] = final_df[numeric_columns].apply(pd.to_numeric, errors='coerce')
    final_df = final_df.drop(['Площадь дома', 'Общая площадь, м2'],  axis=1)
    final_df.columns

    # %%
    final_df['Дата публикации'] = pd.to_datetime(final_df['Дата'], format='%Y-%m-%d').dt.strftime('%d.%m.%Y')

    # %%
    column_name_mapping = {
        "Название": "Тип",
        "Тип жилья": "Тип постройки",
        "Тип строения": "Материал",
        "Этажность дома": "Этажность",
        "Общая площадь": "Площадь",
    }

    # Rename the columns
    final_df.rename(columns=column_name_mapping, inplace=True)

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

    new_df["Источник"] = 'Dom_uz'
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
    name_of_file = "Dom_uz"
    df = pd.DataFrame(new_df)

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