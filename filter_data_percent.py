import pandas as pd
import matplotlib.pyplot as plt
from scipy.stats import linregress
from matplotlib.dates import date2num

# Load the data from CSV file
data = pd.read_csv('data_extraction.csv')

# Ensure 'Date' is in datetime format
data['Date'] = pd.to_datetime(data['Date'])

# Check for a 'Stock' column and group data by 'Stock'
grouped_data = data.groupby('Instrument')

# Initialize a list to store results
results = []

# Iterate through each group
for stock, group in grouped_data:
    # Process in batches of 30
    for i in range(0, len(group), 40):
        batch = group.iloc[i:i + 40]
        if len(batch) < 40:
            continue  # Skip batches that do not have exactly 30 rows

        # Prepare data for regression
        first_25 = batch.head(25)
        x_first_25 = date2num(first_25['Date'])
        y_first_25 = first_25['VWAP']

        # Perform linear regression for the first 25 rows
        slope, intercept, r_value, p_value, std_err = linregress(x_first_25, y_first_25)

        # Prepare data for slope calculation for rows 22-30
        next_9 = batch.iloc[25:30]  # Rows 22-30
        x_next_9 = date2num(next_9['Date'])
        y_next_9 = next_9['VWAP']

        # Perform linear regression for rows 22-30
        slope_22_30, _, _, _, _ = linregress(x_next_9, y_next_9)

        # Calculate percentage change using logarithms
        percentage_change_1_21 = (y_first_25.iloc[-1] - y_first_25.iloc[0]) / y_first_25.iloc[0] * 100
        percentage_change_22_30 = (y_next_9.iloc[-1] - y_next_9.iloc[0]) / y_next_9.iloc[0] * 100

        # Prepare data for saving
        result = {
            'Stock': stock,
            'Batch': i // 40 + 1,
            'Percentage_Change_1_21': percentage_change_1_21,
            'Percentage_Change_22_30': percentage_change_22_30
        }
        results.append(result)

# Convert the results to a DataFrame
results_df = pd.DataFrame(results)

# Save the DataFrame to a CSV file
results_df.to_csv('percentage_changes.csv', index=False)

# Load the data from the CSV file containing the percentage changes
data = pd.read_csv('percentage_changes.csv')

# Calculate the average of the percentage changes
average_percentage_change_1_21 = data['Percentage_Change_1_21'].mean()
average_percentage_change_22_30 = data['Percentage_Change_22_30'].mean()

# Print the average percentage changes
print(f"The average percentage change for rows 1-21 is: {average_percentage_change_1_21}")
print(f"The average percentage change for rows 22-30 is: {average_percentage_change_22_30}")

# Filter the data to determine how many percentage changes are greater than 0.15
filtered_data1 = data[data['Percentage_Change_1_21'] > 3]
count_per_stock1 = filtered_data1.groupby('Stock').size()

# Print the number of percentage changes greater than 0.13 per stock
print("Number of percentage changes greater than 0.13 per stock for rows 1-21:")
print(count_per_stock1)

# Filter the stocks with counts greater than 10
stocks_with_gt_10 = count_per_stock1[count_per_stock1 > 10].index

# Load the original data again to filter it based on stocks
original_data = pd.read_csv('data_extraction.csv')
original_data['Date'] = pd.to_datetime(original_data['Date'])

# Filter original data to include only those stocks
filtered_original_data = original_data[original_data['Instrument'].isin(stocks_with_gt_10)]

# Save the filtered data to a new CSV file
filtered_original_data.to_csv('filtered_data_percent.csv', index=False)
