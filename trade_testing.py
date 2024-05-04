import pandas as pd

# Load the data from CSV files
filtered_data = pd.read_csv('filtered_data_percent.csv')

# Convert 'Date' column to datetime
filtered_data['Date'] = pd.to_datetime(filtered_data['Date'])

# Initialize delta
delta = 0.02

# Prepare a list to store the results
results = []

# Group data into chunks of 40 rows
chunk_size = 40
num_chunks = len(filtered_data) // chunk_size + (len(filtered_data) % chunk_size > 0)
for chunk_index in range(num_chunks):
    start_index = chunk_index * chunk_size
    end_index = min((chunk_index + 1) * chunk_size, len(filtered_data))
    chunk_data = filtered_data.iloc[start_index:end_index]

    # Loop through each row of the chunk
    for i, row in chunk_data.iterrows():
        # Only consider rows 25-30 of each group of 40
        if i < start_index + 24:
            continue
        if i > start_index + 29:
            break

        # Calculate limit order price
        limit_order_price = row['Close Price'] * (1 - delta)

        # Find the date of the 30th row
        end_date = chunk_data.iloc[29]['Date']

        # Find the subsequent prices until the 30th row date
        date_filter = (filtered_data['Date'] > row['Date']) & (filtered_data['Date'] <= end_date) & (
            filtered_data['Instrument'] == row['Instrument'])
        subsequent_prices = filtered_data[date_filter]

        # Check if there are enough subsequent rows to process
        if len(subsequent_prices) > 0:
            # Check if the low price triggers the limit order
            trigger_date_row = subsequent_prices[
                subsequent_prices['Low Price'] <= limit_order_price].head(1)

            if not trigger_date_row.empty:
                trigger_date = trigger_date_row.iloc[0]['Date']

                # Calculate exit price (5 percent higher than the limit order price)
                exit_price = limit_order_price * (1 + 0.03)

                # Check if exit price is lower than the high price between rows 30-40
                high_prices_between_30_40 = chunk_data.iloc[30:40]['High Price']
                first_exit_trigger_date_index = high_prices_between_30_40[high_prices_between_30_40 > exit_price].index.min()

                if pd.isnull(first_exit_trigger_date_index):
                    # Set 'It did not fill' as the first trigger exit date
                    first_exit_trigger_date = 'It did not fill'

                    # Get the closing price from the filtered data of row 40 of the corresponding batch
                    first_trigger_exit_price = filtered_data.iloc[end_index - 1]['Close Price']
                else:
                    # Get the date of the first high price exceeding the exit price
                    first_exit_trigger_date = filtered_data.iloc[first_exit_trigger_date_index]['Date']

                    # Calculate the exit price using the exit_price calculated above
                    first_trigger_exit_price = exit_price

                # Calculate profit/loss in dollars
                profit_loss_dollars = first_trigger_exit_price - limit_order_price

                # Calculate profit/loss in percentage
                profit_loss_percent = (profit_loss_dollars / limit_order_price) * 100

                result = {
                    'Stock': row['Instrument'],
                    'Date': row['Date'],
                    'Batch': chunk_index + 1,  # Add batch number
                    'Limit Order Price': limit_order_price,
                    'First Trigger Date': trigger_date,
                    'Low Price': trigger_date_row.iloc[0]['Low Price'],
                    'Exit Price': exit_price,
                    'First Trigger Exit Date': first_exit_trigger_date,
                    'First Trigger Exit Price': first_trigger_exit_price,
                    'Profit/Loss (Dollars)': profit_loss_dollars,
                    'Profit/Loss (%)': profit_loss_percent
                }
                results.append(result)

# Convert the results to a DataFrame
results_df = pd.DataFrame(results)

# Save the DataFrame to a CSV file
results_df.to_csv('first_trigger_dates_grouped_middle.csv', index=False)

# Print summary
total_profit_loss_dollars = results_df['Profit/Loss (Dollars)'].sum()
average_profit_loss_percent = results_df['Profit/Loss (%)'].mean()

print("Summary:")
print(f"Total Profit/Loss (Dollars): {total_profit_loss_dollars}")
print(f"Average Profit/Loss (%): {average_profit_loss_percent}")

print("Results saved to 'first_trigger_dates_grouped_percent_middle.csv'.")
