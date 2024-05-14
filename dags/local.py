def main():

    # %%
    import pandas as pd
    from datetime import datetime

    import pandas as pd
    from sqlalchemy import create_engine

    # Define your database connection parameters
    db_params = {
        'NAME': 'rirppnta',
        'USER': 'rirppnta',
        'PASSWORD': '7v25FdS9q7Hjq7mitKtFjJRTaj-0CfXZ',
        'HOST': 'abul.db.elephantsql.com',
        'PORT': '5432',
    }

    # Create a database connection string
    connection_string = f"postgresql://{db_params['USER']}:{db_params['PASSWORD']}@{db_params['HOST']}:{db_params['PORT']}/{db_params['NAME']}"

    # Create a SQLAlchemy engine
    engine = create_engine(connection_string)

    # Define your SQL query
    sql_query = """
    SELECT id, "views", floor, szhil, skitchen, roof_heigh, plan, sanuzel, balcon, type_remon, series, data_end, infrastr, type_build, rooms, vtor_perv, source_id, source_url, source_dat, price_mete, price_sot, price_obje, appointmen, "type", opisanie1, opisanie2, etaj, dop_k_domu, sotki_ijs, actual_dat, source_nam, xcoord, ycoord, district, "operator"
    FROM "data".cenovaya_setka_jiloy;
    """

    # Execute the SQL query and retrieve data into a DataFrame
    jiloy = pd.read_sql_query(sql_query, engine)

    # %%
    russian_column_names = {
        'views': 'Просмотры',
        'floor': 'Этаж',
        'szhil': 'Площадь',
        'skitchen': 'Площадь кухни',
        'roof_heigh': 'Высота потолков',
        'plan': 'Планировка',
        'sanuzel': 'Санузел',
        'balcon': 'Балкон',
        'type_remon': 'Тип ремонта',
        'series': 'Серия дома',
        'data_end': 'Дата завершения',
        'infrastr': 'Инфраструктура',
        'type_build': 'Тип строения',
        'rooms': 'Количество комнат',
        'vtor_perv': 'Вторичка/первичка',
        'source_id': 'Идентификатор источника',
        'source_url': 'URL источника',
        'source_dat': 'Дата источника',
        'price_mete': 'Цена за метр',
        'price_sot': 'Цена за сотку',
        'price_obje': 'Цена',
        'appointmen': 'Назначение',
        'type': 'type',
        'etaj': 'Этажность',
        'dop_k_domu': 'Дополнения к дому',
        'sotki_ijs': 'Сотки или кв. м',
        'actual_dat': 'Дата публикации',
        'source_nam': 'Источник',
        'xcoord': 'Долгота',
        'ycoord': 'Широта',
        'district': 'Район',
        'operator': 'Оператор'
    }

    # Assuming your DataFrame is named 'jiloy'
    jiloy['Название'] = jiloy['opisanie1'].astype(str) + ' ' + jiloy['opisanie2'].astype(str)

    # Drop the original columns if needed
    jiloy = jiloy.drop(['opisanie1', 'opisanie2'], axis=1)
    jiloy['actual_dat'] = pd.to_datetime(jiloy['actual_dat']).dt.strftime('%d-%m-%Y')
    jiloy['actual_dat'] = pd.to_datetime(jiloy['actual_dat'], format='%d-%m-%Y').dt.strftime('%d.%m.%Y')

    jiloy = jiloy[jiloy['type'].str.lower() != 'аренда']

    jiloy = jiloy.rename(columns=russian_column_names)
    final_df = jiloy.drop(['id', 'Просмотры', 'Высота потолков', 'Балкон', 'Дополнения к дому', 'Оператор', 'Инфраструктура'],  axis=1)
    pd.set_option('display.max_columns', None)

    # %%
    column_name_mapping = {
        "Назначение": "Тип",
        "Вторичка/первичка": "Тип постройки",
        "Тип ремонта": "Ремонт", 
        "Тип строения": "Материал",
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
            
    new_df["Источник"] = 'Local'
    new_df["Валюта"] = 'USD'
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
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import create_engine

    name_of_table = "local"
    df = pd.DataFrame(df_no_duplicates)

    # Create SQLAlchemy engine
    engine = create_engine("postgresql://username:password@hostname:port/database_name")

    with engine.connect() as connection:
        result = connection.execute(
            sqlalchemy.text(
                f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = :table_name AND table_schema = 'data')"
            ),
            table_name=name_of_table,
        )
        table_exists = result.scalar()

    if table_exists:
        # Append new data to existing table
        df.to_sql(name_of_table, engine, if_exists='append', index=False, schema='data')

        # Remove duplicates and update the table
        with engine.connect() as connection:
            connection.execute(
                sqlalchemy.text(
                    f"DELETE FROM data.{name_of_table} WHERE ctid NOT IN (SELECT MIN(ctid) FROM data.{name_of_table} GROUP BY *)"
                )
            )

        print(f"New data appended to existing table '{name_of_table}' and duplicates removed.")
    else:
        # Create table with new data
        df.to_sql(name_of_table, engine, index=False, schema='data')
        print(f"Table '{name_of_table}' created with new data.")

main()