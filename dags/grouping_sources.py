def main():

    # %%
    import os
    import pandas as pd

    # %%
    def concatenate_excels(folder_path, output_file):
        combined_data = pd.DataFrame()
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                if file.endswith('.xlsx') or file.endswith('.xls'):
                    file_path = os.path.join(root, file)
                    excel_data = pd.read_excel(file_path)
                    combined_data = pd.concat([combined_data, excel_data], ignore_index=True)
        combined_data.to_excel(output_file, index=False)

    folder_path = '../Excels/'
    output_file = '../Data/Combined.xlsx'
    concatenate_excels(folder_path, output_file)