import pandas as pd
import os

data_folder = "data"  # Replace with your actual data folder path

directory_path = os.path.join(data_folder)
print(directory_path)
food = pd.read_csv(os.path.join(directory_path, 'food.csv'))
food_nutrient = pd.read_csv(os.path.join(directory_path, 'food_nutrient.csv'), low_memory=False)
nutrient = pd.read_csv(os.path.join(directory_path, 'nutrient.csv'))

# pre-process food names and categories
food_category = pd.read_csv(os.path.join(directory_path, 'food_category.csv'))
food_category = food_category.rename(columns={"description": "CATEGORY"})

# BRANDED PRODUCTS ARE VERY UNSTABLE
# food = food[food["data_type"]!="branded_food"] # Can uncomment this to get rid of branded products
food = food.drop_duplicates(subset=["fdc_id","data_type","description","food_category_id"])
print(f'In total, {food["fdc_id"].nunique()} unique products were found')

# pre-process food nutrients
#### WHAT ABOUT THE DUPLICATES?? (AROUND 2400, ALL BRANDED FOODS, WHAT SHOULD WE DO WITH THEM?)
# By now, let's assume that the latest is correct.
food_nutrient = food_nutrient.drop_duplicates(subset=["fdc_id", "nutrient_id"], keep="last")

merged_nutrients = food_nutrient[["fdc_id", "nutrient_id", "amount"]] \
    .merge(nutrient[["id", "name", "unit_name", "nutrient_nbr"]],
            left_on='nutrient_id', right_on='id', how="left") \
    .merge(food[["fdc_id", "data_type", "description", "food_category_id"]],
            left_on="fdc_id", right_on="fdc_id", how="right")

merged_nutrients = merged_nutrients.rename(columns={
    'description': 'FOOD_NAME',
    'name': 'NUTRIENT',
    'amount': 'value',
    'unit_name': 'UNIT',
    'data_type': 'DATA_TYPE'})

food_category['id'] = food_category['id'].astype(str)
merged_nutrients['food_category_id'] = merged_nutrients['food_category_id'].astype(str)

merged_nutrients = merged_nutrients.merge(food_category[["id", "CATEGORY"]],
                                                left_on='food_category_id', right_on='id', how="left")

desired_columns = ['fdc_id', 'DATA_TYPE', 'CATEGORY', 'FOOD_NAME', 'NUTRIENT', 'UNIT', 'value']
merged_nutrients = merged_nutrients[desired_columns]

pivoted = merged_nutrients.pivot(index=['fdc_id', 'DATA_TYPE', 'CATEGORY', 'FOOD_NAME'], columns=['NUTRIENT', 'UNIT'], values=['value'])

pivoted_copy = pd.DataFrame(pivoted.values, columns=[str(i[1]) + '(' + str(i[2]) + ')' for i in pivoted.columns], index=pivoted.index)
pivoted_copy.to_csv("combined_pivoted_data.csv")

print(f"After processing, {pivoted_copy.shape[0]} products, with {pivoted_copy.shape[1]} columns")
