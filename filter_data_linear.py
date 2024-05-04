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

        # Prepare data for saving
        result = {
            'Stock': stock,
            'Batch': i // 40 + 1,
            'Slope_1_21': slope,
            'Slope_22_30': slope_22_30
        }
        results.append(result)

        # Plot the data points and linear regression line for one batch of one stock
        if stock == 'AMD' and i // 40 + 1 == 1:  # Change the stock and batch number as needed
            # Plot the data points
            plt.figure(figsize=(10, 6))
            plt.scatter(first_25['Date'], first_25['VWAP'], color='blue', label='Data')

            # Plot the linear regression line
            plt.plot(first_25['Date'], slope * x_first_25 + intercept, color='red', label='Linear Regression (1-21)')
            plt.plot(next_9['Date'], slope_22_30 * x_next_9 + intercept, color='green', label='Linear Regression (22-30)')

            # Add labels and title
            plt.xlabel('Date')
            plt.ylabel('VWAP')
            plt.title(f'Linear Regression for Batch {i // 40 + 1} of Stock {stock}')

            # Add legend
            plt.legend()

            # Show plot
            plt.show()

# Save results to a CSV file
equations_df = pd.DataFrame(results)
equations_df.to_csv('equations.csv', index=False)

# Load the data from the CSV file containing the slopes
data = pd.read_csv('equations.csv')

# Calculate the average of the slopes
average_slope_1_21 = data['Slope_1_21'].mean()
average_slope_22_30 = data['Slope_22_30'].mean()

# Print the average slopes
print(f"The average of slopes for rows 1-21 is: {average_slope_1_21}")
print(f"The average of slopes for rows 22-30 is: {average_slope_22_30}")


# Determine how many slopes in each stock are greater than 0.15
filtered_data1 = data[data['Slope_1_21'] > 0.13]
count_per_stock1 = filtered_data1.groupby('Stock').size()

filtered_data2 = data[data['Slope_22_30'] < 0]
count_per_stock2 = filtered_data2.groupby('Stock').size()

# Print the number of slopes greater than 0.15 per stock
print("Number of slopes greater than 0.13 per stock:")
print(count_per_stock1)
print(count_per_stock2)

# Filter the stocks with counts greater than 10
stocks_with_gt_10 = count_per_stock1[(count_per_stock1 > 10) & (count_per_stock2 > 10)].index

# Load the original data again to filter it based on stocks
original_data = pd.read_csv('data_extraction.csv')
original_data['Date'] = pd.to_datetime(original_data['Date'])

# Filter original data to include only those stocks
filtered_original_data = original_data[original_data['Instrument'].isin(stocks_with_gt_10)]

# Save the filtered data to a new CSV file
filtered_original_data.to_csv('filtered_data.csv', index=False)
