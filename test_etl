import pandas as pd

# 1. Extraction (from CSV, replace with your source)
def extract_data(file_path):
    print("Extracting data...")
    df = pd.read_csv(file_path)
    print(f"Rows extracted: {len(df)}")
    return df

# 2. Staging (data cleaning and basic transformation)
def stage_data(df):
    print("Staging data...")
    # example cleaning: remove rows with nulls, convert order_date
    df = df.dropna()
    df['order_date'] = pd.to_datetime(df['order_date'])
    df['quantity'] = df['quantity'].astype(int)
    df['price'] = df['price'].astype(float)
    # Add total price column
    df['total_price'] = df['quantity'] * df['price']
    print(f"Rows after cleaning: {len(df)}")
    return df

# Save the staging data (optional)
def save_staging(df, file_path):
    df.to_csv(file_path, index=False)
    print(f"Staging data saved to {file_path}")

# 3. Data Mart (aggregation: sales by customer and month)
def build_data_mart(staged_df):
    print("Building data mart...")
    staged_df['month'] = staged_df['order_date'].dt.to_period('M').astype(str)
    data_mart = staged_df.groupby(['customer_id', 'month'])['total_price'].sum().reset_index()
    data_mart.rename(columns={'total_price': 'monthly_sales'}, inplace=True)
    print("Data mart created.")
    return data_mart

# Save the data mart (optional)
def save_datamart(df, file_path):
    df.to_csv(file_path, index=False)
    print(f"Data mart saved to {file_path}")

# Main flow
if __name__ == "__main__":
    # Step 1: Extract
    raw_df = extract_data('raw_data.csv')  # <-- replace with your file/source

    # Step 2: Stage
    staging_df = stage_data(raw_df)
    save_staging(staging_df, 'staging_data.csv')

    # Step 3: Data Mart
    datamart_df = build_data_mart(staging_df)
    save_datamart(datamart_df, 'data_mart.csv')

    print('ETL process completed.')
