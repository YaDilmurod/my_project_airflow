def main():
    # %%
    import pandas as pd
    import geopandas as gpd
    from shapely.geometry import Point
    import numpy as np

    # %%
    data = pd.read_excel("..\Data\Combined.xlsx")

    df = pd.DataFrame(data)
    df = df[df['Цена'] != 0]
    df.shape
    df = df.head(100)

    # %%
    column_translation = {
        'Источник': 'source',
        'Название': 'title',
        'Тип': 'type',
        'Санузел': 'bathroom',
        'Тип постройки': 'building_type',
        'Материал': 'material',
        'Широта': 'lat',
        'Долгота': 'long',
        'Район': 'district',
        'Этаж': 'floor',
        'Этажность': 'num_of_floors',
        'Ремонт': 'renovation',
        'Площадь': 'area',
        'Количество комнат': 'num_of_rooms',
        'Дата публикации': 'publication_date',
        'Валюта': 'currency',
        'Цена': 'price',
        'Дата создания': 'created_date'
    }

    df = df.rename(columns=column_translation)

    df = df[['source', 'type', 'building_type',
        'lat', 'long', 'district', 'floor', 'num_of_floors', 'renovation',
        'area', 'num_of_rooms', 'publication_date', 'currency', 'price']]

    # %%
    init_len = len(df)

    subset_columns = ['type', 'building_type', 'district', 'floor', 'num_of_floors', 'renovation',
            'area', 'num_of_rooms']

    df = df.drop_duplicates(subset=subset_columns)

    after_len = len(df)
    print("Number of duplicates removed:", init_len - after_len)

    # %%
    usd_mask = df['currency'] == "USD"
    df.loc[usd_mask, 'price'] *= 12500
    df.loc[usd_mask, 'currency'] = "UZS"

    # %%
    df['publication_date'] = pd.to_datetime(df['publication_date'], format='%d.%m.%Y')
    df['year'] = df['publication_date'].dt.year
    df['year'] = df['year'].fillna(np.nan).astype(float).astype('Int64')

    # %%
    df = df[['source', 'type', 'building_type',
        'lat', 'long', 'district', 'floor', 'num_of_floors', 'renovation',
        'area', 'num_of_rooms', 'currency', 'price', 'year']]

    # %%
    district_borders = gpd.read_file('../Data/district_borders.json')
    df['geometry'] = df.apply(lambda row: Point(row['long'], row['lat']), axis=1)
    gdf = gpd.GeoDataFrame(df, geometry='geometry')
    joined = gpd.sjoin(gdf, district_borders, how="left", op='within')
    df['district'] = df['district'].fillna(joined['NOMI'])
    df.drop(columns=['geometry'], inplace=True)

    # %%
    df['type'] = df['type'].str.lower()

    corrections = {
        'квартира': 'квартира',
        'квартира': 'квартира',
        'частный дом': 'частный',
        'земля': 'частный',
        'участок': 'частный',
        'евро дом': 'частный',
        'дом': 'частный',
        'частный дом на продажу':'частный',
        'квартира во вторичке на продажу': 'квартира',
        'квартира в новостройке на продажу': 'квартира',
        'дача на продажу': 'частный',
    }                                       

    df['type'].replace(corrections, inplace=True)

    allowed_categories = corrections.values()
    df = df[df['type'].isin(allowed_categories)]
    df.reset_index(drop=True, inplace=True)

    # %%
    df['building_type'] = df['building_type'].str.lower()

    corrections = {
        'Новострой': 'первичный',
        'Вторичный': 'вторичный',
        'Вторичка': 'вторичный',
        'Вторичный рынок': 'вторичный',
        'Новостройки': 'первичный',
        'Первичный': 'первичный',
        'вторичный': 'вторичный',
        'Новостройка': 'первичный',
        'первичный': 'первичный',
        'первычный': 'первичный',
        'Вторичний':  'вторичный',
        'торичный': 'вторичный',
        'Вторичные': 'вторичный',
        'Вторичный': 'вторичный',
        'вторичный рынок': 'вторичный',
        'новостройка': 'первичный',
        'новостройки': 'первичный',
        'новострой': 'первичный',
        'вторичка': 'вторичный',
    }

    df['building_type'].replace(corrections, inplace=True)
    allowed_categories = corrections.values()
    df = df[df['building_type'].isin(allowed_categories)]
    df.reset_index(drop=True, inplace=True)

    # %%
    df['renovation'] = df['renovation'].str.lower()

    corrections = {
        'евро ремонт': 'евроремонт',
        'квро ремонт': 'евроремонт',
        'евро ремонт': 'евроремонт',
        'с ремонтом': 'средний ремонт',
        'требуется ремонт': 'нужен ремонт',
        'требует ремонта': 'нужен ремонт',
        'средняя': 'средний ремонт',
        'среднее состояние': 'средний ремонт',
        'требует ремонта': 'нужен ремонт',
        'незаконченный евроремонт': 'нужен ремонт',
        'требует ремонта':  'нужен ремонт',
        'дизайнерский': 'евроремонт',
        'не требуется': 'средний ремонт',
        'косметический': 'евроремонт',
        'черновая отделка': 'нужен ремонт',
        'коробка': 'нужен ремонт',
        'без ремонта': 'нужен ремонт',
        'капитальный ремонт': 'евроремонт',
    }

    df['renovation'].replace(corrections, inplace=True)
    allowed_categories = corrections.values()
    df = df[df['renovation'].isin(allowed_categories)]
    df.reset_index(drop=True, inplace=True)

    # %%
    # Group by 'district' column and count the number of rows in each group
    district_counts = df.groupby('district').size()

    # Display the count of rows in each district
    district_counts

    # %%
    df_map = pd.read_excel('..\Data\Map_Data.xlsx')
    categories_to_keep = ['Школа', 'Супермаркет', 'Магазин', 'Частная школа', 'Средняя школа', 'Начальная школа',
                        'Международная школа', 'Торговый центр', 'Продовольственный магазин',
                        'Магазин шаговой доступности', 'Супермаркет низких цен', 'Ресторан', 'фастфуд',
                        'Узбекская кухня','Кафе','Суши','Турецкая кухня','Гамбургеры','Корейская кухня','Японская кухня','Еда на вынос','Доставка готовой еды','Парк']

    df_map = df_map[df_map['category'].isin(categories_to_keep)]

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
        'Ресторан':'food', 
        'фастфуд':'food',
        'Узбекская кухня':'food',
        'Кафе':'food',
        'Суши':'food',
        'Турецкая кухня':'food',
        'Гамбургеры':'food',
        'Корейская кухня':'food',
        'Японская кухня':'food',
        'Еда на вынос':'food',
        'Доставка готовой еды':'food',
        'Парк': 'park'
    }

    df_map['category'].replace(corrections, inplace=True)

    df_map.dropna(subset=['category', 'lat', 'long'], inplace=True)

    df_map.drop_duplicates(subset=['category', 'lat', 'long'], inplace=True)

    # %%
    # def haversine(lat1, lon1, lat2, lon2):
    #     lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    #     dlon = lon2 - lon1
    #     dlat = lat2 - lat1
    #     a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    #     c = 2 * np.arcsin(np.sqrt(a))
    #     r = 6371 
    #     return c * r

    # categories = df_map['category'].unique()
    # for cat in categories:
    #     df[cat] = 0

    # for i, row_df in df.iterrows():
    #     print(i)
    #     for j, row_map in df_map.iterrows():
    #         dist = haversine(row_df['lat'], row_df['long'], row_map['lat'], row_map['long'])
    #         if dist <= 2:
    #             df.at[i, row_map['category']] += 1


    # %%
    # weights = {'school': 0.5, 'grocery': 0.5, 'mall': 0.3, 'food': 0.3, 'park': 0.5}
    # df['comfortability_index'] = df['school'] * weights['school'] + \
    #                               df['grocery'] * weights['grocery'] + \
    #                               df['mall'] * weights['mall'] + \
    #                               df['food'] * weights['food'] + \
    #                               df['park'] * weights['park']

    # %%
    # df = df[df['comfortability_index'] >= 0]

    # %%
    for column in df.columns:
        mode_value = df[column].mode()[0]
        df[column] = df[column].fillna(mode_value)

    # %%
    # df_price = pd.read_excel('..\Data\Pricing.xlsx')
    # df = pd.merge(df, df_price, on=['year', 'type', 'building_type'], how='left')

    # %%
    def remove_outliers(df, column, threshold):
        # Convert column to numeric type if necessary
        df[column] = pd.to_numeric(df[column], errors='coerce')
        
        # Calculate mean and standard deviation
        mean_val = df[column].mean()
        std_val = df[column].std()
        
        # Calculate lower and upper bounds for outliers
        lower_bound = mean_val - threshold * std_val
        upper_bound = mean_val + threshold * std_val
        
        # Remove outliers
        df = df[(df[column] >= lower_bound) & (df[column] <= upper_bound)]
        
        return df


    df = remove_outliers(df, 'price', 3)
    df = remove_outliers(df, 'num_of_rooms', 3)
    df = remove_outliers(df, 'area', 3)
    df.reset_index(drop=True, inplace=True)

    # %%
    df = df[['renovation',	'district', 'area',	'num_of_rooms', 'type', 'building_type', 'price']]

    # %%
    def encode_and_drop(df, column_name):
        one_hot_encoded = pd.get_dummies(df[column_name], prefix=column_name)
        one_hot_encoded = one_hot_encoded.astype(int)
        
        df = pd.concat([df, one_hot_encoded], axis=1)
        
        df.drop(column_name, axis=1, inplace=True)
        
        return df

    columns_to_process = ['district', 'renovation', 'type', 'building_type']
    for column in columns_to_process:
        df = encode_and_drop(df, column)


    # %%
    df.columns

    # %%
    excel_file_path = "..\\Data\\Cleaned_Combined.xlsx"
    existing_data = pd.read_excel(excel_file_path)
    combined_data = pd.concat([existing_data, df])
    combined_data.drop_duplicates(keep='first', inplace=True)
    combined_data.to_excel(excel_file_path, index=False)


