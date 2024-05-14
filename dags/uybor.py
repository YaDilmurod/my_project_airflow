def main():

    # %%
    import requests
    import pandas as pd
    from datetime import datetime

    # %%
    category_mapping = {
        7: "Квартира",
        8: "Дом",
        10: "Для бизнеса",
        11: "Земельный участок"
    }

    state_mapping = {
        False: "Новострой",
        True: "Вторичка",
    }

    def get_json_data(api_url):
        response = requests.get(api_url)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error {response.status_code}: Unable to fetch data from {api_url}")
            return None

    def create_dataframe(json_data):
        if json_data and 'results' in json_data:
            results = json_data['results']
            if not results:
                print("No results found. Skipping to the next iteration.")
                return pd.DataFrame()

            apartments = []
            for result in results:
                apartment = {
                    "id": result.get("id"),
                    "categoryId": category_mapping.get(result.get("categoryId")),
                    "description": result.get("description"),
                    "price": result.get("price"),
                    "priceCurrency": result.get("priceCurrency"),
                    "address": result.get("address"),
                    "region": result["region"]["name"]["en"] if result.get("region") else None,
                    "district": result["district"]["name"]["en"] if result.get("district") else None,
                    "street": result["street"]["name"]["en"] if result.get("street") else None,
                    "zone": result["zone"]["name"]["en"] if result.get("zone") else None,
                    "room": result.get("room"),
                    "square": result.get("square"),
                    "floor": result.get("floor"),
                    "floorTotal": result.get("floorTotal"),
                    "isNewBuilding": state_mapping.get(result.get("isNewBuilding")),
                    "repair": result.get("repair"),
                    "foundation": result.get("foundation"),
                    "residentialComplex": result["residentialComplex"]["name"]["en"] if result.get("residentialComplex") else None,
                    "lat": result.get("lat"),
                    "lng": result.get("lng"),
                    "createdAt": result.get("createdAt"),
                    "updatedAt": result.get("updatedAt"),
                    "isUrgently": result.get("isUrgently"),
                    "isVip": result.get("isVip"),
                    "isPremium": result.get("isPremium"),
                    "isFavorite": result.get("isFavorite"),
                    "views": result.get("views"),
                    "clicks": result.get("clicks"),
                    "favorites": result.get("favorites")
                }
                apartments.append(apartment)
            return pd.DataFrame(apartments)
        else:
            print("Invalid JSON data. Skipping to the next iteration.")
            return pd.DataFrame()

    def fetch_data(start_page, end_page):
        all_data = []

        for page_number in range(start_page, end_page):
        
            # Construct the API URL for the current page number and page
            api_url = f"{url_template}{page_number}"

            print(api_url)
            # Fetch JSON data
            json_data = get_json_data(api_url)

            # Check if the response is empty or results are empty
            if not json_data or not json_data.get('results'):
                print(f"No results for page={page_number}. Exiting the loop.")
                break

            # Create DataFrame
            df = create_dataframe(json_data)

            # Append DataFrame to the list
            all_data.append(df)
        
        # Concatenate all DataFrames into a single DataFrame
        final_df = pd.concat(all_data, ignore_index=True)
        return final_df

    # Example for room__in=1
    url_template = "https://api.uybor.uz/api/v1/listings?mode=search&includeFeatured=true&limit=20&embed=category%2CsubCategory%2CresidentialComplex%2Cregion%2Ccity%2Cdistrict%2Czone%2Cstreet%2Cmetro%2Cmedia%2Cuser%2Cuser.avatar%2Cuser.organization%2Cuser.organization.logo&order=upAt&operationType__eq=sale&priceCurrency__eq=usd&page="

    start_page = 1
    end_page = 400

    final_df = fetch_data(start_page, end_page)

    # %%
    final_df = final_df.drop(["id", 'isVip', 'isPremium', 'isFavorite', 'views', 'clicks', 'favorites', 'street', 'zone', 'residentialComplex'], axis=1)
    final_df = final_df.rename(columns={"floor": "Этаж", "floorTotal": "Этажность", "repair": "Ремонт",
                                        "foundation": "Материал", 'room': "Количество комнат", "square": "Площадь", 'region': 'Регион',
                                        'description': 'Название', 'price': 'Цена', 'priceCurrency': 'Валюта',
                                        'district': "Район", 'address': 'Адрес', 'isNewBuilding': 'Новое здание',
                                        'isUrgently': 'Срочно', "createdAt": "Дата публикации", "updatedAt": "Обновлён",
                                        "lat": "Широта", "lng": "Долгота", "categoryId": "Тип", "isNewBuilding": "Тип постройки"})

    final_df['Дата публикации'] = pd.to_datetime(final_df['Дата публикации'], utc=True)
    final_df['Обновлён'] = pd.to_datetime(final_df['Обновлён'], utc=True)
    final_df['Валюта'] = final_df['Валюта'].str.upper()
    repair_mapping = {
        'evro': 'Евроремонт',
        'chernovaya': 'Черновая отделка',
        'custom': 'Индивидуальный',
        'kapital': 'Капитальный',
        'sredniy': 'Средний',
        None: None  # Keep None values unchanged
    }
    material_mapping = {
        'kirpich': 'Кирпич',
        'monolit': 'Монолит',
        'panel': 'Панель',
        'other': 'Другой',
        None: None  # Keep None values unchanged
    }
    final_df['Ремонт'] = final_df['Ремонт'].replace(repair_mapping)
    final_df['Материал'] = final_df['Материал'].replace(material_mapping)
    final_df['Дата публикации'] = final_df['Дата публикации'].dt.strftime('%d.%m.%Y')
    final_df['Обновлён'] = final_df['Обновлён'].dt.strftime('%d.%m.%Y')

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

    
    new_df["Источник"] = 'Uybor'
    new_df["Район"] = ''
    new_df["Дата создания"] = datetime.now().strftime("%d.%m.%Y")
    new_df[columns_to_check]

    # %%
    import pandas as pd
    import os

    # Assuming x is your variable and data is the data you want to store
    name_of_file = "Uybor"
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



