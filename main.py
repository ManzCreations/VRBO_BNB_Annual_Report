import logging

import pandas as pd
from openpyxl.styles import numbers


def setup_logging():
    logging.basicConfig(level=logging.INFO, format='\033[37m%(message)s\033[0m')

    # Create a custom formatter for error messages
    error_formatter = logging.Formatter('\033[91m%(levelname)s: %(message)s\033[0m')

    # Create a console handler for error messages and set the formatter
    error_handler = logging.StreamHandler()
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(error_formatter)

    # Add the error handler to the root logger
    logging.getLogger().addHandler(error_handler)


def debug(message):
    logging.info(message)


def error(message):
    logging.error(message)


def merge_bnb(current: pd.DataFrame, bnb: pd.DataFrame) -> pd.DataFrame:
    try:
        # Filter the desired columns in the bnb dataframe
        bnb = bnb[['Code', 'Customer', 'Type', 'Listing', 'Amount']]
        # Filter rows where Type is either "Reservation" or "Adjustment"
        bnb = bnb[(bnb['Type'] == 'Reservation') | (bnb['Type'] == 'Adjustment')]

        # Merge the dataframes for Airbnb
        merged_df_bnb = pd.merge(current, bnb, on='Code', how='left')
        merged_df_bnb = merged_df_bnb[
            ['Code', 'VRBO_ID', 'QBO', 'Cleaning', 'Tax_Location', 'Type', 'Listing', 'Amount']]

        # Check for missing codes in bnb and try to match based on other columns
        missing_codes_bnb = set(bnb['Code']) - set(current['Code'])

        for code in missing_codes_bnb:
            listing_match = current[current['ListingBNB'] == bnb[bnb['Code'] == code]['Listing'].values[0]]
            if not listing_match.empty:
                merged_df_bnb.loc[merged_df_bnb['Code'].isnull() & (
                        merged_df_bnb['Listing'] == bnb[bnb['Code'] == code]['Listing'].values[0]), 'Code'] = code
            else:
                customer_match = current[current['QBO'] == bnb[bnb['Code'] == code]['Customer'].values[0]]
                if not customer_match.empty:
                    merged_df_bnb.loc[merged_df_bnb['Code'].isnull() & (
                            merged_df_bnb['QBO'] == bnb[bnb['Code'] == code]['Customer'].values[0]), 'Code'] = code

        # Check for any remaining missing codes
        remaining_missing_codes_bnb = set(bnb['Code']) - set(merged_df_bnb['Code'])
        if remaining_missing_codes_bnb:
            debug("Warning: The following codes from BNB are not found in Current:")
            debug(remaining_missing_codes_bnb)

        return merged_df_bnb
    except Exception as e:
        error(f"Error in merge_bnb: {str(e)}")
        return pd.DataFrame()


def merge_vrbo(current: pd.DataFrame, vrbo: pd.DataFrame) -> pd.DataFrame:
    try:
        # Filter the desired columns in the vrbo dataframe
        vrbo = vrbo[['Code', 'Customer', 'Property ID', 'Payout']]

        # Merge the dataframes for VRBO
        merged_df_vrbo = pd.merge(current, vrbo, on='Code', how='left')

        # Check if 'VRBO_ID' column exists in the current DataFrame
        if 'VRBO_ID' in current.columns:
            merged_df_vrbo = merged_df_vrbo[
                ['Code', 'ListingBNB', 'QBO', 'Cleaning', 'Tax_Location', 'VRBO_ID', 'Payout']]
        else:
            merged_df_vrbo = merged_df_vrbo[['Code', 'ListingBNB', 'QBO', 'Cleaning', 'Tax_Location', 'Payout']]

        # Check for missing codes in vrbo and try to match based on other columns
        missing_codes_vrbo = set(vrbo['Code']) - set(current['Code'])

        for code in missing_codes_vrbo:
            listing_match = current[current['VRBO_ID'] == vrbo[vrbo['Code'] == code]['Property ID'].values[
                0]] if 'VRBO_ID' in current.columns else pd.DataFrame()
            if not listing_match.empty:
                merged_df_vrbo.loc[merged_df_vrbo['Code'].isnull() & (
                        merged_df_vrbo['VRBO_ID'] == vrbo[vrbo['Code'] == code]['Property ID'].values[
                    0]), 'Code'] = code
            else:
                customer_match = current[current['QBO'] == vrbo[vrbo['Code'] == code]['Customer'].values[0]]
                if not customer_match.empty:
                    merged_df_vrbo.loc[merged_df_vrbo['Code'].isnull() & (
                            merged_df_vrbo['QBO'] == vrbo[vrbo['Code'] == code]['Customer'].values[0]), 'Code'] = code

        # Check for any remaining missing codes
        remaining_missing_codes_vrbo = set(vrbo['Code']) - set(merged_df_vrbo['Code'])
        if remaining_missing_codes_vrbo:
            debug("Warning: The following codes from VRBO are not found in Current:")
            debug(remaining_missing_codes_vrbo)

        return merged_df_vrbo
    except Exception as e:
        error(f"Error in merge_vrbo: {str(e)}")
        return pd.DataFrame()


def calculate_taxes(merged_df_bnb: pd.DataFrame, merged_df_vrbo: pd.DataFrame) -> pd.DataFrame:
    try:
        # Create a list of all listings found in both bnb and vrbo
        listings = list(set(merged_df_bnb['Listing']).union(set(merged_df_vrbo['ListingBNB'])))

        # Create the taxes dataframe
        taxes_df = pd.DataFrame(
            columns=['Code', 'Listing', 'VRBO_ID', 'QBO', 'Tax_Location', 'Number_of_Cleanings', 'Total_Cleaning',
                     'Total_Income', 'Total_Taxes'])

        for listing in listings:
            bnb_data = merged_df_bnb[merged_df_bnb['Listing'] == listing].copy()
            vrbo_data = merged_df_vrbo[merged_df_vrbo['ListingBNB'] == listing].copy()

            # Convert 'Cleaning' column to numeric type
            bnb_data.loc[:, 'Cleaning'] = pd.to_numeric(bnb_data['Cleaning'], errors='coerce')
            vrbo_data.loc[:, 'Cleaning'] = pd.to_numeric(vrbo_data['Cleaning'], errors='coerce')

            cleaning_rows = bnb_data[bnb_data['Type'] != 'Adjustment'].shape[0] + vrbo_data.shape[0]
            total_cleaning = bnb_data[bnb_data['Type'] != 'Adjustment']['Cleaning'].sum() + vrbo_data['Cleaning'].sum()
            total_income = bnb_data['Amount'].sum() + vrbo_data['Payout'].sum()
            total_taxes = 0.00558 * (total_income - total_cleaning)

            code = ''
            vrbo_id = ''
            qbo = ''
            tax_location = ''

            if not bnb_data.empty:
                code = bnb_data['Code'].iloc[0]
                vrbo_id = bnb_data['VRBO_ID'].iloc[0] if 'VRBO_ID' in bnb_data.columns else ''
                qbo = bnb_data['QBO'].iloc[0]
                tax_location = bnb_data['Tax_Location'].iloc[0]
            elif not vrbo_data.empty:
                code = vrbo_data['Code'].iloc[0]
                vrbo_id = vrbo_data['VRBO_ID'].iloc[0] if 'VRBO_ID' in vrbo_data.columns else ''
                qbo = vrbo_data['QBO'].iloc[0]
                tax_location = vrbo_data['Tax_Location'].iloc[0]

            new_row = pd.DataFrame({
                'Code': [code],
                'Listing': [listing],
                'VRBO_ID': [vrbo_id],
                'QBO': [qbo],
                'Tax_Location': [tax_location],
                'Number_of_Cleanings': [cleaning_rows],
                'Total_Cleaning': [total_cleaning],
                'Total_Income': [total_income],
                'Total_Taxes': [total_taxes]
            })

            taxes_df = pd.concat([taxes_df, new_row], ignore_index=True, sort=False)

        # Remove rows with all empty, NaN, or 0 values
        taxes_df.dropna(how='all', inplace=True)
        taxes_df = taxes_df[(taxes_df != 0).any(axis=1)]

        # Remove the first row from taxes_df
        taxes_df = taxes_df.iloc[1:]

        # Set the dtypes of columns
        taxes_df['Code'] = taxes_df['Code'].astype(str)
        taxes_df['Listing'] = taxes_df['Listing'].astype(str)
        taxes_df['VRBO_ID'] = taxes_df['VRBO_ID'].astype(str)
        taxes_df['QBO'] = taxes_df['QBO'].astype(str)
        taxes_df['Tax_Location'] = taxes_df['Tax_Location'].astype(str)
        taxes_df['Number_of_Cleanings'] = taxes_df['Number_of_Cleanings'].astype(int)
        taxes_df['Total_Cleaning'] = taxes_df['Total_Cleaning']
        taxes_df['Total_Income'] = taxes_df['Total_Income']
        taxes_df['Total_Taxes'] = taxes_df['Total_Taxes']

        # Sort taxes_df by 'Code' column
        taxes_df.sort_values('Code', inplace=True)

        # Save taxes_df to Excel using openpyxl
        output_path = "Final_Taxes_2023.xlsx"
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            taxes_df.to_excel(writer, sheet_name="Taxes 2024", index=False)

            # Set the number format for specific columns
            workbook = writer.book
            worksheet = writer.sheets['Taxes 2024']
            worksheet.column_dimensions['C'].number_format = numbers.FORMAT_TEXT
            worksheet.column_dimensions['G'].number_format = numbers.FORMAT_CURRENCY_USD_SIMPLE
            worksheet.column_dimensions['H'].number_format = numbers.FORMAT_CURRENCY_USD_SIMPLE
            worksheet.column_dimensions['I'].number_format = numbers.FORMAT_CURRENCY_USD_SIMPLE

        debug(f"Taxes DataFrame saved to {output_path}")

        return taxes_df
    except Exception as e:
        error(f"Error in calculate_taxes: {str(e)}")
        return pd.DataFrame()


def main(bnb: pd.DataFrame, vrbo: pd.DataFrame, current: pd.DataFrame):
    setup_logging()

    try:
        # Filter the desired columns in the current dataframe
        current = current[['Code', 'ListingBNB', 'VRBO_ID', 'QBO', 'Cleaning', 'Tax_Location']]

        debug("Merging BNB data...")
        merged_df_bnb = merge_bnb(current, bnb)
        debug(f"Merged BNB DataFrame shape: {merged_df_bnb.shape}")

        debug("Merging VRBO data...")
        merged_df_vrbo = merge_vrbo(current, vrbo)
        debug(f"Merged VRBO DataFrame shape: {merged_df_vrbo.shape}")

        debug("Calculating taxes...")
        taxes_df = calculate_taxes(merged_df_bnb, merged_df_vrbo)
        debug(f"Taxes DataFrame shape: {taxes_df.shape}")

        debug("Saving taxes DataFrame to Excel...")
        taxes_df.to_excel("Final_Taxes_2023.xlsx", sheet_name="Taxes 2024", index=False)

        debug("Process completed successfully.")
    except Exception as e:
        error(f"Error in main: {str(e)}")


if __name__ == "__main__":
    try:
        bnb_df = pd.read_excel(r"C:\Users\manzf\Downloads\Re_ Help with totals for 2023\airbnb_ANNUAL_2023.xlsx",
                               sheet_name="airbnb_01_2023-12_2023redo_234")
        vrbo_df = pd.read_excel(r"C:\Users\manzf\Downloads\Re_ Help with totals for 2023\VRBO_ANNUAL_2023.xlsx",
                                sheet_name="VRBOredo-234")
        current_df = pd.read_excel(r"C:\Users\manzf\Downloads\Re_ Help with totals for 2023\Current_by_unit.xlsx",
                                   sheet_name="Current By Unit")
        main(bnb_df, vrbo_df, current_df)
    except FileNotFoundError as e:
        error(f"Error: {str(e)}. Please check the file paths and try again.")
    except Exception as e:
        error(f"Unexpected error: {str(e)}")
