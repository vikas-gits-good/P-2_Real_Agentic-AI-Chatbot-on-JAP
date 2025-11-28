import pandas as pd


df = pd.read_excel("./src/Data/Raw_Data_Links.xlsx", sheet_name="CSJ")
df.fillna("", inplace=True)
my_dict = df.to_dict(orient="index")
my_dict
