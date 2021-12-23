# Import libraries
import pandas as pd

# Import dataset
df_LOD = pd.read_csv(
    r"D:/Python Projects/Data Mining-Tubes/data/raw/List of Orders.csv"
)
df_OD = pd.read_csv(r"D:/Python Projects/Data Mining-Tubes/data/raw/Order Details.csv")
df_ST = pd.read_csv(r"D:/Python Projects/Data Mining-Tubes/data/raw/Sales Target.csv")

# Data cleaning: remove missing values
df_LOD.dropna(inplace=True)

# Data transformation
df_LOD["Order Date"] = pd.to_datetime(df_LOD["Order Date"])
df_ST["Month of Order Date"] = pd.to_datetime(
    df_ST["Month of Order Date"], format="%b-%y"
)

# Data integration
df_orders = (
    pd.merge(df_LOD, df_OD, on="Order ID")
    .groupby(["Order ID", "Order Date", "CustomerName", "State", "City"])
    .agg(
        {
            "Amount": lambda x: x.tolist(),
            "Profit": lambda x: x.tolist(),
            "Quantity": lambda x: x.tolist(),
            "Category": lambda x: x.tolist(),
            "Sub-Category": lambda x: x.tolist(),
        }
    )
)
df_target = df_ST.groupby(["Month of Order Date"]).agg(
    {"Category": lambda x: x.tolist(), "Target": lambda x: x.tolist()}
)

# Export the output to csv files
df_orders.to_csv(
    "D:\\Python Projects\\Data Mining-Tubes\\data\\post-preprocessing/orders.csv"
)
df_target.to_csv(
    "D:\\Python Projects\\Data Mining-Tubes\\data\\post-preprocessing/targets.csv"
)
