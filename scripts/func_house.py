

def merge_data():
    reviewer_metrics_2024 = pd.read_csv("reviewer-metrics.csv")
    updated_file = pd.read_csv("file_list.csv")

    merged_df = pd.merge(updated_file, reviewer_metrics_2024, on=['Name','MS Number','Version','Year','Editor','Journal'], how='outer')
    merged_df = merged_df.dropna(subset=['Year'])
    merged_df['Year'] = merged_df['Year'].astype(int)
    # Save the merged dataframe to CSV
    current_date = datetime.now().strftime('%Y-%m-%d')
    filename = f"merged_{current_date}.csv"
    df_sorted = merged_df.sort_values(by='Year', ascending=False)
    df_sorted.to_csv(filename, index=False)
    print("File saved")
