def main():

    # %%
    import requests
    from bs4 import BeautifulSoup
    import pandas as pd
    from urllib.parse import urljoin
    from datetime import datetime

    # %%
    districts_to_check = [
                                'Алмазарский район',
                                'Бектемирский район',
                                'Мирабадский район',
                                'Мирзо-Улугбекский район',
                                'Сергелийский район',
                                'Учтепинский район',
                                'Чиланзарский район',
                                'Шайхантахурский район',
                                'Юнусабадский район',
                                'Яккасарайский район',
                                'Яшнабадский район',
                                'Янгихаётский район'
    ]

    # %%
    import requests
    from requests.exceptions import ReadTimeout, RequestException
    from bs4 import BeautifulSoup
    import pandas as pd
    from urllib.parse import urljoin
    from datetime import datetime
    import time

    # Function to convert the date string to the desired format
    def convert_date_format(input_date):
        # Check if the date is "Сегодня в hh:mm"
        if "Сегодня" in input_date:
            current_date = datetime.now().strftime("%d.%m.%Y")
            formatted_date = f"{current_date}"
        else:
            month_dict = {
                'января': '01', 'февраля': '02', 'марта': '03',
                'апреля': '04', 'мая': '05', 'июня': '06',
                'июля': '07', 'августа': '08', 'сентября': '09',
                'октября': '10', 'ноября': '11', 'декабря': '12'
            }
            for ru_month, num_month in month_dict.items():
                input_date = input_date.replace(ru_month, num_month)

            formatted_date = datetime.strptime(input_date, "%d %m %Y г.").strftime("%d.%m.%Y")[:10]

        return pd.to_datetime(formatted_date, format="%d.%m.%Y")

    def scrape_apartment_details(main_url, num_pages):
        results = []

        try:
            for page in range(1, num_pages):
                current_url = f"{main_url}?page={page}"
                print(main_url, page)
                try:
                    response = requests.get(current_url, timeout=5)
                    response.raise_for_status()  # Raise an exception for HTTP errors

                    soup = BeautifulSoup(response.text, 'html.parser')

                    # Find all apartment links inside the specified div
                    apartment_links = soup.select('div.css-u2ayx9 a')
                    regions = soup.select('div.css-odp1qd > p.css-1a4brun')
                    # Loop through each apartment link
                    for link, region in zip(apartment_links, regions):
                        apartment_url = urljoin(main_url, link['href'])
                        apartment_response = requests.get(apartment_url)
                        if apartment_response.status_code == 200:
                            apartment_soup = BeautifulSoup(apartment_response.text, 'html.parser')

                            # Extract the information from the detailed page
                            title_element = apartment_soup.find('h4', class_='css-1juynto')

                            region_text = region.text.strip()
                            for district in districts_to_check:
                                if district in region_text:
                                    region = district
                                    break

                            if title_element:
                                apartment_title = title_element.text.strip()

                                # Find the price container element in the detailed apartment page
                                price_container_element = apartment_soup.find('div', {'data-testid': 'ad-price-container'})

                                if price_container_element:
                                    # Find the h3 element within the price container
                                    price_element = price_container_element.find('h3')

                                    # Extract and print the text content of the h3 element
                                    apartment_price = price_element.text.strip() if price_element else None

                                    # Extract currency and numeric part
                                    currency = 'UZS' if 'сум' in apartment_price else 'USD'
                                    numeric_part = ''.join(filter(str.isdigit, apartment_price))

                                # Extract negotiability
                                negotiable_element = apartment_soup.find('div', {'data-testid': 'ad-price-container'})

                                if negotiable_element:
                                    negotiable_element = negotiable_element.find('p')
                                    negotiable = 'Yes' if negotiable_element and 'Договорная' in negotiable_element.text else 'No'
                                else:
                                    negotiable = 'No'

                                # Extract description from div with data-cy="ad_description"
                                description_element = apartment_soup.find('div', {'data-cy': 'ad_description'})
                                apartment_description = description_element.text.strip() if description_element else None

                                # Extract additional information with handling if element doesn't exist
                                details_element = apartment_soup.select_one('ul.css-sfcl1s')
                                details = {}

                                if details_element:
                                    for li in details_element.find_all('li'):
                                        li_text = li.text.strip()
                                        if ':' in li_text:
                                            column_name, content = li_text.split(':', 1)
                                            details[column_name.strip()] = content.strip()

                                # Extract date from span with data-cy="ad-posted-at"
                                date_element = apartment_soup.find('span', {'data-cy': 'ad-posted-at'})
                                posted_at = date_element.text.strip() if date_element else None

                                # Use the function to convert the date
                                posted_at = convert_date_format(posted_at)

                                result_entry = {
                                    'Название': apartment_title,
                                    'Валюта': currency,
                                    'Цена': numeric_part,
                                    'Цена_договорная': negotiable,
                                    'Описание': apartment_description[8:],
                                    'Дата': posted_at,
                                    'Район': region,
                                    **details
                                }
                                results.append(result_entry)
                                print(apartment_url)

                        else:
                            print(f"Failed to retrieve details from link: {apartment_url}. Status code: {apartment_response.status_code}")

                except ReadTimeout as e:
                    print(f"ReadTimeout exception: {e}. Moving to the next iteration.")
                    time.sleep(5)  # Add a delay before retrying, adjust as needed
                    continue  # Move to the next iteration of the loop

                except requests.RequestException as e:
                    print(f"RequestException: {e}")
                    # Handle other request exceptions if needed

        except Exception as e:
            print(f"An unexpected error occurred: {e}")

        finally:
            # Create a DataFrame from the results list
            final_df = pd.DataFrame(results)
            return final_df

    main_url = "https://www.olx.uz/nedvizhimost/kvartiry/prodazha/tashkent/"
    kvartiri_df = scrape_apartment_details(main_url, num_pages=30)
    kvartiri_df["Тип"] = 'Квартира'

    # %%
    import requests
    from requests.exceptions import ReadTimeout, RequestException
    from bs4 import BeautifulSoup
    import pandas as pd
    from urllib.parse import urljoin
    from datetime import datetime
    import time

    # Function to convert the date string to the desired format
    def convert_date_format(input_date):
        # Check if the date is "Сегодня в hh:mm"
        if "Сегодня" in input_date:
            current_date = datetime.now().strftime("%d.%m.%Y")
            formatted_date = f"{current_date}"
        else:
            month_dict = {
                'января': '01', 'февраля': '02', 'марта': '03',
                'апреля': '04', 'мая': '05', 'июня': '06',
                'июля': '07', 'августа': '08', 'сентября': '09',
                'октября': '10', 'ноября': '11', 'декабря': '12'
            }
            for ru_month, num_month in month_dict.items():
                input_date = input_date.replace(ru_month, num_month)

            formatted_date = datetime.strptime(input_date, "%d %m %Y г.").strftime("%d.%m.%Y")[:10]

        return pd.to_datetime(formatted_date, format="%d.%m.%Y")

    def scrape_apartment_details(main_url, num_pages):
        results = []

        try:
            for page in range(1, num_pages):
                current_url = f"{main_url}?page={page}"

                try:
                    response = requests.get(current_url, timeout=5)
                    response.raise_for_status()  # Raise an exception for HTTP errors

                    soup = BeautifulSoup(response.text, 'html.parser')

                    # Find all apartment links inside the specified div
                    apartment_links = soup.select('div.css-u2ayx9 a')
                    regions = soup.select('div.css-odp1qd > p.css-1a4brun')

                    # Loop through each apartment link
                    for link, region in zip(apartment_links, regions):
                        apartment_url = urljoin(main_url, link['href'])
                        apartment_response = requests.get(apartment_url)
                        if apartment_response.status_code == 200:
                            apartment_soup = BeautifulSoup(apartment_response.text, 'html.parser')

                            # Extract the information from the detailed page
                            title_element = apartment_soup.find('h4', class_='css-1juynto')

                            region_text = region.text.strip()
                            for district in districts_to_check:
                                if district in region_text:
                                    region = district
                                    break

                            if title_element:
                                apartment_title = title_element.text.strip()

                                # Find the price container element in the detailed apartment page
                                price_container_element = apartment_soup.find('div', {'data-testid': 'ad-price-container'})

                                if price_container_element:
                                    # Find the h3 element within the price container
                                    price_element = price_container_element.find('h3')

                                    # Extract and print the text content of the h3 element
                                    apartment_price = price_element.text.strip() if price_element else None

                                    # Extract currency and numeric part
                                    currency = 'UZS' if 'сум' in apartment_price else 'USD'
                                    numeric_part = ''.join(filter(str.isdigit, apartment_price))

                                # Extract negotiability
                                negotiable_element = apartment_soup.find('div', {'data-testid': 'ad-price-container'})

                                if negotiable_element:
                                    negotiable_element = negotiable_element.find('p')
                                    negotiable = 'Yes' if negotiable_element and 'Договорная' in negotiable_element.text else 'No'
                                else:
                                    negotiable = 'No'

                                # Extract description from div with data-cy="ad_description"
                                description_element = apartment_soup.find('div', {'data-cy': 'ad_description'})
                                apartment_description = description_element.text.strip() if description_element else None

                                # Extract additional information with handling if element doesn't exist
                                details_element = apartment_soup.select_one('ul.css-sfcl1s')
                                details = {}

                                if details_element:
                                    for li in details_element.find_all('li'):
                                        li_text = li.text.strip()
                                        if ':' in li_text:
                                            column_name, content = li_text.split(':', 1)
                                            details[column_name.strip()] = content.strip()

                                # Extract date from span with data-cy="ad-posted-at"
                                date_element = apartment_soup.find('span', {'data-cy': 'ad-posted-at'})
                                posted_at = date_element.text.strip() if date_element else None

                                # Use the function to convert the date
                                posted_at = convert_date_format(posted_at)

                                result_entry = {
                                    'Название': apartment_title,
                                    'Валюта': currency,
                                    'Цена': numeric_part,
                                    'Цена_договорная': negotiable,
                                    'Описание': apartment_description[8:],
                                    'Дата': posted_at,
                                    'Район': region,
                                    **details
                                }
                                results.append(result_entry)
                                print(apartment_url)

                        else:
                            print(f"Failed to retrieve details from link: {apartment_url}. Status code: {apartment_response.status_code}")

                except ReadTimeout as e:
                    print(f"ReadTimeout exception: {e}. Moving to the next iteration.")
                    time.sleep(5)  # Add a delay before retrying, adjust as needed
                    continue  # Move to the next iteration of the loop

                except requests.RequestException as e:
                    print(f"RequestException: {e}")
                    # Handle other request exceptions if needed

        except Exception as e:
            print(f"An unexpected error occurred: {e}")

        finally:
            # Create a DataFrame from the results list
            final_df = pd.DataFrame(results)
            return final_df

    main_url = "https://www.olx.uz/nedvizhimost/doma/prodazha/tashkent/"
    dom_df = scrape_apartment_details(main_url, num_pages=10)
    dom_df["Тип"] = 'Дом'

    # %%
    final_df = pd.concat([dom_df, kvartiri_df], ignore_index=True)

    # %%
    final_df['Дата'] = pd.to_datetime(final_df['Дата'], format='%Y-%m-%d').dt.strftime('%d.%m.%Y')
    final_df['Общая площадь'] = final_df['Общая площадь'].str.extract('(\d+)').astype(float)

    # %%
    column_name_mapping = {
        "Назначение": "Тип",
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

    
    new_df["Источник"] = 'Olx'
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
    name_of_file = "Olx"
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



