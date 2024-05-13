from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'scrapping_dom_uz',
    default_args=default_args,
    description='A DAG to scrape data from dom.uz',
    schedule_interval='@daily',
    catchup=False
)

def run_scraper():
    import requests
    from bs4 import BeautifulSoup
    from datetime import datetime
    import pandas as pd

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

run_scraper_task = PythonOperator(
    task_id='run_scraper',
    python_callable=run_scraper,
    dag=dag
)
