def main():

    # %%
    from sklearn.preprocessing import MinMaxScaler
    from sklearn.model_selection import train_test_split, GridSearchCV
    from sklearn.linear_model import LinearRegression
    from sklearn.metrics import mean_squared_error
    import joblib
    import pandas as pd

    # %%
    df = pd.read_excel('..\Data\Cleaned_Combined.xlsx')

    columns_to_scale = ['num_of_rooms', 'area']
    scaler = MinMaxScaler()
    X_scaled = scaler.fit_transform(df[columns_to_scale])
    X_scaled_df = pd.DataFrame(X_scaled, columns=columns_to_scale)
    df = pd.concat([X_scaled_df, df.drop(columns_to_scale, axis=1)], axis=1)

    # %%
    X = df.drop('price', axis=1)
    y = df['price']

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    param_grid = {'fit_intercept': [True, False]}
    grid_search = GridSearchCV(LinearRegression(), param_grid, cv=5, scoring='neg_mean_squared_error')
    grid_search.fit(X_train, y_train)

    best_params = grid_search.best_params_

    best_model = LinearRegression(**best_params)
    best_model.fit(X_train, y_train)

    y_pred = best_model.predict(X_test)
    mse = mean_squared_error(y_test, y_pred)

    joblib.dump(best_model, '..\Data\linear_regression_model.pkl')



