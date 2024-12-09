import pandas as pd
import os

def clean_earnings_data(input_csv, output_csv):
    """
    Cleans the earnings data by:
    - Selecting only 'Period' and 'Amount' columns based on their positions.
    - Removing the '$' symbol from the Amount column.
    - Converting the Amount column to float.
    - Saving the cleaned data to a new CSV.
    
    Parameters:
    - input_csv: str, path to the raw earnings_data.csv
    - output_csv: str, path to save the cleaned earnings_data_cleaned.csv
    """
    try:
        # Read the raw earnings data with tab as the separator
        # Use the 'python' engine to handle multiple tabs gracefully
        df = pd.read_csv(input_csv, sep='\t', engine='python', header=0)

        print(f"Original data shape: {df.shape}")
        print(f"Columns found: {df.columns.tolist()}")  # Debugging line to show columns

        # Show first few rows for debugging
        print("\nFirst 5 rows of the raw data:")
        print(df.head())

        # Check the number of columns
        num_cols = df.shape[1]
        print(f"\nNumber of columns read: {num_cols}")

        # If there are more than 3 columns, likely due to trailing tabs, ignore the extra columns
        if num_cols > 3:
            df = df.iloc[:, :3]  # Keep only first 3 columns
            print(f"Reduced data to first 3 columns. New shape: {df.shape}")

        # Verify that 'Period' and 'Amount' columns exist
        if 'Period' not in df.columns or 'Amount' not in df.columns:
            print("\nError: 'Period' and/or 'Amount' columns not found.")
            print("Available columns:", df.columns.tolist())
            return

        # Select only 'Period' and 'Amount' columns by position to avoid misalignment
        df_selected = df[['Period', 'Amount']].copy()
        print("\nSelected 'Period' and 'Amount' columns.")

        # Remove the '$' symbol and any leading/trailing whitespace from 'Amount'
        df_selected['Amount'] = df_selected['Amount'].astype(str).str.replace('[\$,]', '', regex=True).str.strip()
        
        # Debugging: Print unique values in 'Amount' to identify any unexpected entries
        unique_amounts = df_selected['Amount'].unique()
        print("\nUnique values in 'Amount' column after cleaning:")
        print(unique_amounts)

        # Check for non-numeric 'Amount' entries
        # Allow for decimal points by removing them temporarily
        df_selected['Amount_numeric'] = df_selected['Amount'].str.replace('.', '', regex=False).str.isdigit()
        non_numeric_amount = df_selected[~df_selected['Amount_numeric']]

        if not non_numeric_amount.empty:
            print("\nFound non-numeric values in 'Amount' column:")
            print(non_numeric_amount)
            # Remove these rows
            df_selected = df_selected[df_selected['Amount_numeric']].copy()
            df_selected.drop('Amount_numeric', axis=1, inplace=True)
            print(f"Removed {len(non_numeric_amount)} rows with non-numeric 'Amount' values.")
        else:
            print("\nAll 'Amount' entries are numeric.")
            df_selected.drop('Amount_numeric', axis=1, inplace=True)

        # Convert 'Amount' to float
        df_selected['Amount'] = df_selected['Amount'].astype(float)
        print("Converted 'Amount' column to float.")

        # Drop any rows with missing 'Period' or 'Amount' after cleaning
        initial_rows = df_selected.shape[0]
        df_selected = df_selected.dropna(subset=['Period', 'Amount']).reset_index(drop=True)
        final_rows = df_selected.shape[0]
        if final_rows < initial_rows:
            print(f"Removed {initial_rows - final_rows} rows due to missing 'Period' or 'Amount' values.")
        else:
            print("No missing values detected in 'Period' or 'Amount' columns.")

        # Save the cleaned data to a new CSV with only 'Period' and 'Amount'
        df_selected.to_csv(output_csv, index=False)
        print(f"\nCleaned data saved to {output_csv}")

    except FileNotFoundError:
        print(f"\nError: The file {input_csv} does not exist.")
    except pd.errors.EmptyDataError:
        print("\nError: The input CSV file is empty.")
    except KeyError as e:
        print(f"\nError: Missing expected column {e}")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")

if __name__ == "__main__":
    # Define paths relative to the script location
    script_dir = os.path.dirname(os.path.abspath(__file__))
    input_csv = os.path.join(script_dir, '..', 'data', 'earnings_raw.csv')  # Adjusted to 'earnings_data.csv'
    output_csv = os.path.join(script_dir, '..', 'data', 'earnings_data_cleaned.csv')

    # Clean the data
    clean_earnings_data(input_csv, output_csv)