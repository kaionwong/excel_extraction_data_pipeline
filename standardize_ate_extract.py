import sys
import os
import re
import pandas as pd
import numpy as np
import math
import random
import warnings
import random
from datetime import datetime as dt
from dateutil import parser
import datetime
import logging

warnings.simplefilter(action='ignore', category=UserWarning)

# Control panel
save_csv_switch = True
save_log_switch = True
# Can add items in the lists below, the program will iterate through them in the respective subdirectory
full_municipality_list = ['beaumont', 'calgary', 'camrose', 'canmore_town', 'coaldale', 'devon', 
                          'edmonton', 'edson', 'fort_saskatchewan', 'grande_prairie', 'hinton', 
                          'lethbridge', 'lloydminster', 'medicine_hat', 'morinville', 'red_deer',
                          'slave_lake', 'spruce_grove', 'st_albert', 'stony_plain', 'strathcona_county',
                          'taber', 'wainwright', 'wetaskiwin', 'whitecourt']
random_sample_n = random.randint(1, len(full_municipality_list))
sample_loc_list = random.sample(full_municipality_list, random_sample_n)
test_loc_list = ['_test']
sp_loc_list = ['taber', 'edmonton']
municipality_list = full_municipality_list
year_list = [2022, 2023]

# Helper function
def pandas_output_setting():
    """Set pandas output display setting"""
    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', None)
    ##pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 170)
    pd.set_option('display.max_colwidth', None)
    pd.options.mode.chained_assignment = None  # default='warn'

class LoggerWriter:
    def __init__(self, level):
        self.level = level

    def write(self, message):
        # If a message is a blank line, don't log it
        if message != '\n':
            self.level(message.strip())

    def flush(self):
        pass

pandas_output_setting()
os.system('cls')

if save_log_switch:
    # Get the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Create a "log" directory if it doesn't exist
    current_time_string = dt.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_dir = os.path.join(script_dir, 'log')
    os.makedirs(log_dir, exist_ok=True)

    # Configure logging to save output to a file in the "log" directory
    log_file_path = os.path.join(log_dir, f'log_{current_time_string}')
    # configure logging
    # along with log level and time
    logging.basicConfig(filename=log_file_path, level=logging.DEBUG)

    # Redirect stdout to the log file
    sys.stdout = LoggerWriter(logging.info)
    sys.stderr = LoggerWriter(logging.warning)

def count_substrings(input_string):
    # Split the string into substrings
    substrings = input_string.split()

    # Filter out substrings that are not valid (e.g., empty strings)
    valid_substrings = [substring for substring in substrings if substring]

    # Count the number of valid substrings
    count = len(valid_substrings)

    return count

def extract_month(input_string):
    """
        standardize string input into corresponding month: 'asdfasdf' -> None ; 'jan' -> 'Jan', 'March,' -> 'March'
    """
    # Define a regular expression pattern to match month names
    month_pattern = r'\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\b'

    # Search for the month pattern in the input string
    match = re.search(month_pattern, input_string, flags=re.IGNORECASE)

    # Return the matched month or None if not found
    return match.group() if match else None

def month_to_number(month_string):
    """
        convert month string into month number
    """
    if month_string is None:
        return None

    month_mapping = {
        'jan': 1, 'january': 1,
        'feb': 2, 'february': 2,
        'mar': 3, 'march': 3,
        'apr': 4, 'april': 4,
        'may': 5,
        'jun': 6, 'june': 6,
        'jul': 7, 'july': 7,
        'aug': 8, 'august': 8,
        'sep': 9, 'september': 9,
        'oct': 10, 'october': 10,
        'nov': 11, 'november': 11,
        'dec': 12, 'december': 12
    }

    # Convert the input to lowercase for case-insensitive matching
    normalized_month = month_string.lower()

    # Use the mapping or default to None if the month is not found
    return month_mapping.get(normalized_month, None)

def month_to_quarter(month_number):
    """
        convert month number into quarter number
    """
    if month_number is None or not (1 <= month_number <= 12):
        return None  # Return None for invalid month numbers

    # Define quarter ranges
    quarters = {1: 1, 2: 1, 3: 1, 4: 2, 5: 2, 6: 2, 7: 3, 8: 3, 9: 3, 10: 4, 11: 4, 12: 4}

    # Return the corresponding quarter
    return quarters.get(month_number)

def standardize_quarter(quarter_string):
    """
        standardize quarter input into quarter number
    """
    if quarter_string is None:
        return None
    
    quarter_mapping = {
        'q1': 1,
        'q 1': 1,
        'quarter 1': 1,
        'first quarter': 1,
        '1st quarter': 1,
        'quarter 1st': 1,
        
        'q2': 2,
        'q 2': 2,
        'quarter 2': 2,
        'second quarter': 2,
        '2nd quarter': 2,
        'quarter 2nd': 2,

        'q3': 3,
        'q 3': 3,
        'quarter 3': 3,
        'third quarter': 3,
        '3rd quarter': 3,
        'quarter 3rd': 3,
        
        'q4': 4,
        'q 4': 4,
        'quarter 4': 4,
        'fourth quarter': 4,
        '4th quarter': 4,
        'quarter 4th': 4,      
    }

    # Convert the input to lowercase for case-insensitive matching
    normalized_quarter = quarter_string.lower()

    # Use the mapping or default to None if the month is not found
    return quarter_mapping.get(normalized_quarter, None)

def process_lowercase_municipality(input_string):
    new_string = input_string.replace('_', ' ')
    return new_string.title()

def process_excel_worksheet_into_df(df, year, month, quarter, municipality):
    # Step: Identify the Header Row based on Content (i.e., "Site ID" and "Device Type")
    header_row_condition = df.apply(lambda row: row.astype(str).str.contains('Site ID|Device Type').any(), axis=1)   

    try:
        header_row_index = df.index[header_row_condition].tolist()[0]
    except IndexError:
        return df

    # Step: Remove Rows Above Header
    df = df[df.index >= header_row_index]

    # Step: Set Header Row as Column Names
    df.columns = df.iloc[0]
    
    # Drop the duplicate header row
    df = df.iloc[1:]

    # Step: Remove strings inside square brackets and strip leading/trailing whitespaces
    df.columns = [str(col).replace('\n', ' ').split('[')[0].strip() for col in df.columns]
    df.columns = [str(col).replace('\n', ' ').split('(')[0].strip() for col in df.columns]

    # Step: check columns, remove rows if they have missing values in these columns
    columns_to_check = ['Site ID', 'Device Type', 'Location Description']
    # Drop rows where all specified columns have NaN values
    df = df.dropna(subset=columns_to_check, how='all')
    # Reset the index
    df = df.reset_index(drop=True)

    # Step: Add municipality, year, month and quarter
    df['Municipality'] = process_lowercase_municipality(municipality)
    df['Year'] = year
    df['Month'] = month
    df['Quarter'] = quarter
    
    # Rename column names so they match with the typical/majority of the column naming
    df.columns = df.columns.str.upper().str.replace(' ', '_')

    return df

def extract_quarter_from_string(input_string):
    """
        extract quarter string from text
    """
    # Define quarter pattern
    quarter_pattern = re.compile(r'\b(?:q(?:uarter)?\s*[1-4]|[1-4]\s*quarter|first(?:\s*quarter)?|1st(?:\s*quarter)?|second(?:\s*quarter)?|2nd(?:\s*quarter)?|third(?:\s*quarter)?|3(?:rd)?(?:\s*quarter)?|fourth(?:\s*quarter)?|4(?:th)?(?:\s*quarter)?)\b', re.IGNORECASE)

    # Search for quarter pattern in the input string
    match = re.search(quarter_pattern, input_string)

    if match:
        quarter = match.group(0).lower()  # Convert to lowercase for consistency
        if '1' in quarter or 'first' in quarter or '1st' in quarter:
            return 'q1'
        elif '2' in quarter or 'second' in quarter or '2nd' in quarter:
            return 'q2'
        elif '3' in quarter or 'third' in quarter or '3rd' in quarter:
            return 'q3'
        elif '4' in quarter or 'fourth' in quarter or '4th' in quarter:
            return 'q4'
    return None

def pair_value_list(value_list):
    """
        Will be used to add the content from cell to the right to the current cell
    """
    formatted_list = []

    for i in range(len(value_list) - 1):
        # Concatenate adjacent elements into pairs
        pair = f"{value_list[i]} {value_list[i + 1]}"
        formatted_list.append(pair)

    return formatted_list

def process_excel_worksheet_without_time_ref_in_sheetname_into_df(df, year, municipality, device_type=None):
    # def process_excel_worksheet_without_time_ref_in_sheetname_into_df(df, year, month=None, quarter=None, device_type=None):
    # Steps below
    # >> Condition #1 - Identify the header row -> extract into df -> check if "month" or "quarter" column exists -> if so, extract month and/or quarter (if only month exists, derive quarter from month)
    #       >> rename column names so it can be merged with master df with the right column information
    # >> Condition #2 - If there is no "month" or "quarter" columns, seek the quarter reference from the excel cells themselves -> if "quarter" reference exists
    #       >> rename column names so it can be merged with master df with the right column information
    
    single_month_pattern = r'\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\b'
    quarter_pattern = r'^(?:q[1-4]|Q[1-4])$'
    
    extracted_quarter = None
    df_original = df.copy()
    
    # Step: Identify the Header Row based on Content (i.e., "Site ID" and "Device Type")    
    header_row_condition = df.apply(lambda row: row.astype(str).str.lower().str.contains('site name|site id|location selection criteria').any(), axis=1)   

    try:
        header_row_index = df.index[header_row_condition].tolist()[0]
    except IndexError:
        return df

    # Step: Remove Rows Above Header
    df = df[df.index >= header_row_index]

    # Step: Set Header Row as Column Names
    df.columns = df.iloc[0]
    valid_col = df.columns[~df.columns.isna()].tolist()

    # Condition #1 (if 'month' and/or 'quarter' columns exist)
    if any(col.lower() in ['month', 'quarter'] for col in valid_col):
        # Drop the duplicate header row
        df = df.iloc[1:]

        # Step: Remove strings inside square brackets and strip leading/trailing whitespaces
        df.columns = [str(col).replace('\n', ' ').split('[')[0].strip() for col in df.columns]
        df.columns = [str(col).replace('\n', ' ').split('(')[0].strip() for col in df.columns]

        try:
            # Step: check columns, remove rows if they have missing values in these columns
            columns_to_check = ['Location selection criteria', 'Date of last assessment']
            # Drop rows where all specified columns have NaN values
            df = df.dropna(subset=columns_to_check, how='all')
            # Reset the index
            df = df.reset_index(drop=True)
        except Exception as e:
            logging.debug(f'Exception caught: {e}')
            pass

        # Step: Add municipality, year, month and quarter
        df['Municipality'] = process_lowercase_municipality(municipality)
        df['Year'] = year
        
        if 'device type' not in [col.lower() for col in df.columns]: # then standardize value
            df['Device Type'] = device_type.title()

        # Step: handle different setups and input data format
        if 'quarter' in [col.lower() for col in df.columns]: # then standardize value
            df['Quarter'] = df['Quarter'].apply(standardize_quarter)
        
        if 'month' in [col.lower() for col in df.columns]: # then standardize value
            month_col_values = df['Month'].tolist()
            
            # Check if the 10th element is numeric
            is_1st_element_numeric = isinstance(month_col_values[0], (int, float))
            
            if is_1st_element_numeric:        
                df['Month'] = df['Month']
                
            else:
                df['Month'] = df['Month'].apply(extract_month).apply(month_to_number)
        
        if 'quarter' not in [col.lower() for col in df.columns]: # extract quarter from month since month must exists
            df['Quarter'] = df['Month'].apply(month_to_quarter)
        
        if 'month' not in [col.lower() for col in df.columns]: # assign it to None since we can't derive month from quarter only
            df['Month'] = None
        
        # Further cleanup/standardizing the col names
        if 'Date of last assessment' in [col for col in df.columns]:
            df.rename(columns = {
                'Date of last assessment':'Date of Last Assessed'
            }, inplace=True)

        if 'Number of collisions' in [col for col in df.columns]:
            df.rename(columns = {
                'Number of collisions':'Total Number of Collisions'
            }, inplace=True)
            
        if 'Number of fatal collisions' in [col for col in df.columns]:
            df.rename(columns = {
                'Number of fatal collisions':'Total Number of Fatal Collisions'
            }, inplace=True)
            
        if 'Number of fatalities' in [col for col in df.columns]:
            df.rename(columns = {
                'Number of fatalities':'Total Number of Fatalities'
            }, inplace=True)
            
        if 'Number of injuries' in [col for col in df.columns]:
            df.rename(columns = {
                'Number of injuries':'Total Number of Injuries'
            }, inplace=True)
            
        if 'Number of injury collisions' in [col for col in df.columns]:
            df.rename(columns = {
                'Number of injury collisions':'Total Number of Injury Collisions'
            }, inplace=True)
            
        if 'Number of property damage collisions' in [col for col in df.columns]:
            df.rename(columns = {
                'Number of property damage collisions':'Total Number of Property Damage Collisions'
            }, inplace=True)
        
        df.columns = df.columns.str.upper().str.replace(' ', '_')
        
    # Condition #2 (else if both 'month' and 'quarter' columns do not exist)
    else:
        # Since there is no month or quarter reference from the worksheet name or from the column name, we will try to extract it from description above header row
        df_above = df_original[df_original.index < header_row_index]
            
        if len(df_above) >= 1:
            # Extract all values into a flat list
            value_list = df_above.values.flatten().tolist()            
            value_list = [value for value in value_list if not (isinstance(value, float) and math.isnan(value) and np.nan)]
            value_list_merged = pair_value_list(value_list)
            final_value_list = list(set(value_list + value_list_merged))

            # # test
            # final_value_list = ['2023-04-01 to 2023-06-01']

            for value in final_value_list:    
                if (extracted_quarter is None) & (value is not None) & (final_value_list is not None):               
                    month_list = []
                    quarter_list = []
                    extracted_quarter = standardize_quarter(extract_quarter_from_string(str(value)))

                    if extracted_quarter:
                        break

                    else:
                        substrings = str(value).split()
                        for substring in substrings:
                            string_to_month = parse_date(substring)
                            string_to_quarter = extract_quarter_from_string(substring)

                            if string_to_quarter:
                                break
                                
                            if is_year(substring):
                                continue # skip the rest of the cpde below if satisfied
                                
                            if string_to_month:            
                                if re.search(single_month_pattern, string_to_month, re.IGNORECASE):
                                    if string_to_month not in month_list:
                                        month_list.append(string_to_month)
                                    
                                    if string_to_quarter:
                                        quarter_list.append(string_to_month)
                                        
                            if re.search(quarter_pattern, substring, re.IGNORECASE):
                                    quarter_list.append(substring)

                            if quarter_list and not month_list:
                                extracted_quarter = standardize_quarter(extract_quarter_from_string(quarter_list[0]))
                            
                            elif len(month_list) >= 2:
                                # if there are two or more months, I need to divide them into "within the same quarter" or "span over more than 1 quarters"
                                recombined_month_string = ' '.join(month_list)
                                
                                # apply extract_quarter_from_multiple_months() so it returns a specific quarter or None
                                extracted_quarter = standardize_quarter(extract_quarter_from_multiple_months(recombined_month_string))

                            if len(quarter_list) >= 1:
                                extracted_quarter = standardize_quarter(extract_quarter_from_multiple_months(quarter_list[0]))
                                break

    # if there is valid value from extracted_quarter, then extract that and add its df to the master df
    if extracted_quarter:
        # Step: Identify the Header Row based on Content (i.e., "Site ID" and "Device Type")    
        header_row_condition = df.apply(lambda row: row.astype(str).str.lower().str.contains('site name|site id|location selection criteria').any(), axis=1)  

        try: 
            header_row_index = df.index[header_row_condition].tolist()[0]
        except IndexError:
            return df

        # Step: Remove Rows Above Header
        df = df[df.index >= header_row_index]

        # Step: Set Header Row as Column Names
        df.columns = df.iloc[0]
        valid_col = df.columns[~df.columns.isna()].tolist()
        # Drop columns with NaN names
        df = df.dropna(axis=1, how='all')

        # Drop the duplicate header row
        df = df.iloc[1:]

        # Step: Remove strings inside square brackets and strip leading/trailing whitespaces
        df.columns = [str(col).replace('\n', ' ').split('[')[0].strip() for col in df.columns]
        df.columns = [str(col).replace('\n', ' ').split('(')[0].strip() for col in df.columns]

        try:
            # Step: check columns, remove rows if they have missing values in these columns
            columns_to_check = ['Location selection criteria', 'Date of last assessment']
            # Drop rows where all specified columns have NaN values
            df = df.dropna(subset=columns_to_check, how='all')
            # Reset the index
            df = df.reset_index(drop=True)
        except Exception as e:
            logging.debug(f'Exception caught: {e}')
            pass

        # Step: Add municipality, year, month and quarter
        df['Municipality'] = process_lowercase_municipality(municipality)
        df['Year'] = year
        
        # Step: add device type if existed
        if 'device type' not in [col.lower() for col in df.columns]: # then standardize value
            df['Device Type'] = device_type
        
        # Step: handle different setups and input data format
        df['Quarter'] = extracted_quarter

        # Further cleanup/standardizing the col names
        if 'Date of last assessment' in [col for col in df.columns]:
            df.rename(columns = {
                'Date of last assessment':'Date of Last Assessed'
            }, inplace=True)

        if 'Number of collisions' in [col for col in df.columns]:
            df.rename(columns = {
                'Number of collisions':'Total Number of Collisions'
            }, inplace=True)
            
        if 'Number of fatal collisions' in [col for col in df.columns]:
            df.rename(columns = {
                'Number of fatal collisions':'Total Number of Fatal Collisions'
            }, inplace=True)
            
        if 'Number of fatalities' in [col for col in df.columns]:
            df.rename(columns = {
                'Number of fatalities':'Total Number of Fatalities'
            }, inplace=True)
            
        if 'Number of injuries' in [col for col in df.columns]:
            df.rename(columns = {
                'Number of injuries':'Total Number of Injuries'
            }, inplace=True)
            
        if 'Number of injury collisions' in [col for col in df.columns]:
            df.rename(columns = {
                'Number of injury collisions':'Total Number of Injury Collisions'
            }, inplace=True)
            
        if 'Number of property damage collisions' in [col for col in df.columns]:
            df.rename(columns = {
                'Number of property damage collisions':'Total Number of Property Damage Collisions'
            }, inplace=True)

        df.columns = df.columns.str.upper().str.replace(' ', '_')                

    return df

def standardize_month_string(month_string):
    short_month_list = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
    
    # Mapping of full month names to short forms
    month_mapping = {
        'january': 'jan',
        'february': 'feb',
        'march': 'mar',
        'april': 'apr',
        'may': 'may',
        'june': 'jun',
        'july': 'jul',
        'august': 'aug',
        'september': 'sep',
        'october': 'oct',
        'november': 'nov',
        'december': 'dec'
    }

    # Convert the month string to lowercase for case-insensitive comparisons
    month_lower = month_string.lower()

    if month_string in short_month_list:
        return month_string

    else: # Return the corresponding short form or None if not found
        return month_mapping.get(month_lower)   

def extract_quarter_from_multiple_months(input_string):
    # Extract quarter from specific format of month string 'jan feb' and 'jan mar' will give 'q1', 'jan may' will give None since they span over 2 quarters
    # Lower casing input_string so it can match with re patterns easier
    input_string = input_string.lower()
    # Define month pattern
    month_pattern = r'\b(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\b'
    # Convert month names to corresponding quarter numbers
    quarter_mapping = {'q1': ['jan', 'feb', 'mar'], 'q2': ['apr', 'may', 'jun'], 'q3': ['jul', 'aug', 'sep'], 'q4': ['oct', 'nov', 'dec']}
    
    month_ref_count = 0
    
    for substring in input_string.split():
        if re.findall(month_pattern, substring, re.IGNORECASE):
            month_ref_count += 1
    
    if month_ref_count <= 1:
        return None  # No or single month reference, return None
    
    else:
        # since there is more than 1 month references, this will loop through the substrings again and execute mapping from month to quarter
        # then if all mapped quarters are the same, then return the quarter; else (if there is more than 1 quarters), return None because there isn't a single appropriate quarter input
        quarter_bucket = []
        
        for substring in input_string.split():
            substring = standardize_month_string(substring)
            
            # Check if the standardized month is present in any quarter
            for quarter, months in quarter_mapping.items():
                if substring in months:
                    quarter_bucket.append(quarter)
                    
        if len(set(quarter_bucket)) == 1:
            return quarter_bucket[0]
    
        else:
            return None

def parse_date(input_string):
    # check if it is a numeric value only (4 digits), if so, return None
    if input_string.isdigit() and len(input_string) == 4:
        return None
    
    # check if the format is like this '2023-01-23', so if treat it as 'YYYY-MM-DD', then extract month as 3 letter abbreviation
    try:
        date_obj = parser.parse(input_string)
        if isinstance(date_obj, datetime.datetime):  # Make sure it's a valid datetime object
            month_output = date_obj.strftime('%B')
            return month_output[0:3].lower()
    except ValueError:
        pass
    
    # check if it is numeric value only, if so, return None
    try: 
        # address some strange string to month pattern
        if int(input_string):
            return None

    except ValueError:
        pass
    
    try: 
        # address some strange string to month pattern
        if input_string[-2:] in ['st', 'nd', 'rd', 'th']:
            return None

    except:
        pass
    
    try:
        date_obj = parser.parse(input_string)
        month_output = date_obj.strftime('%B')
        return month_output[0:3].lower()
    except ValueError:
        return None  

def is_year(input_string):
    return re.match(r'^\d{4}$', input_string) is not None

def quarter_column_cleanup(value):
    if pd.isna(value) or isinstance(value, float):
        return None  # or any other value you consider as missing
    elif isinstance(value, int):
        return value
    elif isinstance(value, float):
        return int(value)
    elif value.lower() in ['q1', 'q2', 'q3', 'q4']:
        return int(value[1])
    else:
        return None  # or any other value you consider as missing

def standardize_string_into_time(input_string):
    input_substring_list = input_string.split()
    month_list = []
    quarter_list = []
    
    single_month_pattern = r'\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\b'
    quarter_pattern = r'^(?:q[1-4]|Q[1-4])$'

    quarter_string_ref = ['1', '2', '3', '4', 'first', 'second', 'third', 'fourth', 'one', 'two', 'third', 'four', '1st', '2nd', '3rd', '4th']
    
    # takes care of the patterns like "quarter 1" and "1 quarter" 
    for current_index, substring in enumerate(input_substring_list[:-1]):
        next_substring = input_substring_list[current_index + 1]
        combined_substring = f"{substring} {next_substring}"
        
        if substring in quarter_string_ref:
            if next_substring.lower() == 'quarter':
                quarter_list.append(combined_substring)
                
        elif substring == 'quarter':
            if next_substring in quarter_string_ref:
                quarter_list.append(combined_substring)

    # loop through and match the individual substring isolates to month/quarter patterns
    for substring in input_substring_list:
        string_to_month = parse_date(substring)
        string_to_quarter = extract_quarter_from_string(substring)
        
        if is_year(substring):
            continue
        
        if string_to_month:            
            if re.search(single_month_pattern, string_to_month, re.IGNORECASE):
                if string_to_month not in month_list:
                    month_list.append(string_to_month)
            
            if string_to_quarter:
                quarter_list.append(string_to_month)
                
        if re.search(quarter_pattern, substring, re.IGNORECASE):
                quarter_list.append(substring)

    if quarter_list and not month_list:
        return 'quarter'
    
    elif len(month_list) == 1:
        return 'single_month'
    
    elif len(month_list) >= 2:
        # if there are two or more months, I need to divide them into "within the same quarter" or "span over more than 1 quarters"
        recombined_month_string = ' '.join(month_list)
        
        # apply extract_quarter_from_multiple_months() so it returns a specific quarter or None
        extracted_quarter = extract_quarter_from_multiple_months(recombined_month_string)
        
        if extracted_quarter:
            return 'multiple_months'
    
        else:
            'no_time_ref'
    
    else:
        return 'no_time_ref'

# test_list = [
#     'jan 2023',
#     '2021 June',
#     'q1',
#     'first quarter',
#     '1st QUarter',
#     'quarter 4',
#     'quarter 4th June 2023',
#     'q2 2023',
#     'Q4 jan 2023',
#     'jan to march 2023',
#     '2022-07-01',
#     '2022',
#     '2022-07-01 - 2022-08-01',
#     '2022-01-01 - 2022-02-01', # expect return multiple months since jan and feb are from the same quarter
#     '2000-01-01 - 2023-04-23' # expect return none since jan and apr are from different quarters
# ]

# for i in test_list:
#     print('////')
#     print(i)
#     print(standardize_string_into_time(i))

##########
##########
def main():
    # Main code
    # Create main master df so the later dfs from each municipality and year-month can stack on it
    full_column_list = [
        "Municipality", "Year", "Month", "Quarter", "Site Name",
        "Site Id", "Device Type", "Location Description", "Date Location was First Operational", "Location Selection Criteria",
        "Date of Last Assessed", "Direction of Monitored Vehicles", "Speed Limit", "Deployment Hours", "Average Daily Traffic Volume",
        "Number of Vehicles Monitored", "Average Traffic Speed - All Vehicles", "Total Number of Speeding Contraventions", 
        "Total Number of Speed Notices Issued", "Total Number of Red Light Contraventions", "Total Number of Red Light Notices Issued",
        "Total Number of Stop Running Contraventions", "Total Number of Stop Running Notices Issued", "Total Number of Collisions",
        "Total Number of Fatal Collisions", "Total Number of Injury Collisions", "Total Number of Property Damage Collisions", 
        "Total Number of Fatalities", "Total Number of Injuries"]

    full_column_list = [i.upper().replace(' ', '_') for i in full_column_list]

    master_df = pd.DataFrame(columns=full_column_list)

    # Loop through the municipality_list and year_list; 
    # and in each loop, loop through all xlsx files inside a directory; 
    # and in each file, loop through the worksheet tabs
    for curr_municipality in municipality_list:
        for curr_year in year_list:
            municipality_dir_label = [curr_municipality]
            target_year = [str(curr_year)]

            curr_municipality = municipality_dir_label[0]
            curr_year = target_year[0]

            # Get the current directory and then full .csv file path
            curr_dir = os.getcwd()
            data_path = os.path.join(
                curr_dir, 'data', curr_municipality, curr_year)
            result_path = os.path.join(
                curr_dir, 'result')
            
            # Get a list of all files in the directory
            try:
                all_files = os.listdir(data_path)
            except:
                continue
            
            # Filter out files that do not contain "vread" in their names
            excel_file_list = [file for file in all_files if file.lower().endswith((".xlsx", ".xls")) and "vread" not in file.lower()]
                    
            # Loop through the filtered excel files
            for file in excel_file_list:            
                # craete path to a specific xlsx then read (multiple) worksheet's names and content into a dictionary (sheet_dict)
                try:
                    file_path = os.path.join(data_path, file)
                except:
                    continue

                if file.lower().endswith('.xlsx'):
                    sheets_dict = pd.read_excel(file_path, sheet_name=None, header=None)
                elif file.lower().endswith('.xls'):
                    xls_file = pd.ExcelFile(file_path)
                    sheets_dict = {sheet_name: xls_file.parse(sheet_name, header=None) for sheet_name in xls_file.sheet_names}
                else:
                    raise ValueError("Unsupported file format. Only XLSX and XLS files are supported.")
                
                # iterate through the worksheet names, and detect certain conditions
                count_worksheet_with_name_with_single_month = 0 # i.e., "Edmonton Jan 2022"
                count_worksheet_with_name_with_multiple_months = 0 # i.e., "Jan - Mar", "2022-07-01 ~ 2022-09-30", "2000-01-01 - 2023-03-31", "September to December"
                count_worksheet_with_name_with_quarter = 0 # i.e., "Q1", "First quarter", "quarter 1", "1st quarter"
                count_worksheet_with_name_without_time_ref = 0 # i.e., "Program_Name", "Photo Radar Camera", "Intersection Safety Device"
                
                # placeholders to hold worksheet names by above categories
                worksheet_with_single_month = []
                worksheet_with_multiple_months = []
                worksheet_with_quarter = []
                worksheet_without_time_ref = []

                # loop through the worksheet and content from the excel file, to give big picture view of the worksheet in relation to each other
                for sheet_name, df in sheets_dict.items():
                    time_ref_category = standardize_string_into_time(sheet_name)               
                    
                    if time_ref_category == "single_month":
                        worksheet_with_single_month.append(sheet_name)
                        count_worksheet_with_name_with_single_month += 1
                        
                    elif time_ref_category == "multiple_months":
                        worksheet_with_multiple_months.append(sheet_name)
                        count_worksheet_with_name_with_multiple_months += 1
                        
                    elif time_ref_category == "quarter":
                        worksheet_with_quarter.append(sheet_name)
                        count_worksheet_with_name_with_quarter += 1
                        
                    else:
                        worksheet_without_time_ref.append(sheet_name)
                        count_worksheet_with_name_without_time_ref += 1
                
                logging.debug(file_path)
                logging.debug(worksheet_with_single_month)
                logging.debug(worksheet_with_multiple_months)
                logging.debug(worksheet_with_quarter)
                logging.debug(worksheet_without_time_ref)

                logging.debug(f'single month worksheet: {count_worksheet_with_name_with_single_month}')
                logging.debug(f'multiple months worksheet: {count_worksheet_with_name_with_multiple_months}')
                logging.debug(f'quarter worksheet: {count_worksheet_with_name_with_quarter}')
                logging.debug(f'no time ref worksheet: {count_worksheet_with_name_without_time_ref}')
                    
                # Create conditions and corresponding actions
                # Schematic:
                ##########
                ##########
                # 1) if there is 1 or more single-month worksheet, just loop through the single-month worksheet, and ignore all else
                if count_worksheet_with_name_with_single_month >= 1:               
                    for sheet_name, df in sheets_dict.items():
                        # renew
                        curr_month = None
                        curr_quarter = None
                        
                        if sheet_name in worksheet_with_single_month:
                                                    
                            logging.debug(f'Sheet name: {sheet_name}')
                            logging.debug(f'Worksheet with single month: {worksheet_with_single_month}')

                            curr_month = month_to_number(extract_month(sheet_name))
                            curr_quarter = month_to_quarter(curr_month) # quarter is derived from month

                            try:                          
                                # Process each worksheet at a time
                                processed_df = process_excel_worksheet_into_df(df, curr_year, curr_month, curr_quarter, municipality=curr_municipality)
                                
                                # Standardize column names
                                if 'Date of Last Assessment' in processed_df.columns:
                                    processed_df.rename(columns={'Date of Last Assessment':'Date of Last Assessed'}, inplace=True)

                                # Only retain columns in processed_df that exist in df_master's column list
                                processed_df_column = processed_df.columns
                                valid_column = list(set(processed_df_column).intersection(full_column_list))
                                processed_df = processed_df[valid_column]
                                # processed_df = processed_df.reset_index(drop=True) # reset index

                                # Insert the individual df to the master_df
                                master_df = pd.concat([master_df, processed_df], ignore_index=True)
                                
                                logging.debug(f'Processed: {file_path} >>> {sheet_name}')
                                
                                if len(processed_df_column) < len(full_column_list):
                                    logging.debug('Processed worksheet has been added to masterfile, however, there are missing columns')
                                
                            except:
                                logging.debug('Not processed:', file_path, sheet_name)
                
                ##########
                ##########
                # 2) if there is 1 or more quarter worksheet (and no single-month worksheet), just loop through the quarter worksheet, and ignore all else
                # beware that if there are both worksheet with quarter and multi-month naming, only the quarter named worksheet will be processed
                elif count_worksheet_with_name_with_quarter >= 1:
                    for sheet_name, df in sheets_dict.items():
                        # renew
                        curr_month = None
                        curr_quarter = None                  
                        
                        if sheet_name in worksheet_with_quarter:
                                                    
                            logging.debug(f'Sheet name: {sheet_name}')
                            logging.debug(f'Worksheet with quarter: {worksheet_with_quarter}')

                            curr_month = month_to_number(extract_month(sheet_name))
                            curr_quarter = extract_quarter_from_string(sheet_name) # quarter is derived from worksheet name

                            try:                          
                                # Process each worksheet at a time
                                processed_df = process_excel_worksheet_into_df(df, curr_year, curr_month, curr_quarter, municipality=curr_municipality)
                                
                                # Standardize column names
                                if 'Date of Last Assessment' in processed_df.columns:
                                    processed_df.rename(columns={'Date of Last Assessment':'Date of Last Assessed'}, inplace=True)
                            
                                # Only retain columns in processed_df that exist in df_master's column list
                                processed_df_column = processed_df.columns
                                valid_column = list(set(processed_df_column).intersection(full_column_list))
                                processed_df = processed_df[valid_column]
                                # processed_df = processed_df.reset_index(drop=True) # reset index

                                # Insert the individual df to the master_df
                                master_df = pd.concat([master_df, processed_df], ignore_index=True)
                                
                                logging.debug(f'Processed: {file_path} >>> {sheet_name}')
                                
                                if len(processed_df_column) < len(full_column_list):
                                    logging.debug('Processed worksheet has been added to masterfile, however, there are missing columns')
                                
                            except:
                                logging.debug('Not processed:', file_path, sheet_name)
                
                ##########
                ##########
                # 3) if there is 1 or more multi-month worksheet (and no single-month worksheet), just loop through the multi-month worksheet, and ignore all else
                elif count_worksheet_with_name_with_multiple_months >= 1:
                    for sheet_name, df in sheets_dict.items():
                        # renew
                        curr_month = None
                        curr_quarter = None    
                        
                        if sheet_name in worksheet_with_multiple_months:
                            logging.debug(f'Sheet name: {sheet_name}')
                            logging.debug(f'Worksheet with multiple smonths: {worksheet_with_multiple_months}')

                            curr_month = np.nan # if a worksheet is deemed "multi-month" worksheet, that means the specific month reference can't be extracted form the worksheet name
                            
                            # extract quarter from multiple month-labeled worksheet name
                            temp_string = ''
                            multiple_month_substrings = sheet_name.split()
                            for multiple_month_substring in multiple_month_substrings:
                                if parse_date(multiple_month_substring):
                                    temp_string = temp_string + parse_date(multiple_month_substring) + ' ' # constructing something like 'jan mar ', 'oct dec ' so it fits the format for (extract_quarter_from_multiple_months()
                            curr_quarter = extract_quarter_from_multiple_months(temp_string) # quarter is derived from worksheet name

                            try:                          
                                # Process each worksheet at a time
                                processed_df = process_excel_worksheet_into_df(df, curr_year, curr_month, curr_quarter, municipality=curr_municipality)
                                
                                # Standardize column names
                                if 'Date of Last Assessment' in processed_df.columns:
                                    processed_df.rename(columns={'Date of Last Assessment':'Date of Last Assessed'}, inplace=True)
                            
                                # Only retain columns in processed_df that exist in df_master's column list
                                processed_df_column = processed_df.columns
                                valid_column = list(set(processed_df_column).intersection(full_column_list))
                                processed_df = processed_df[valid_column]
                                # processed_df = processed_df.reset_index(drop=True) # reset index

                                # Insert the individual df to the master_df
                                master_df = pd.concat([master_df, processed_df], ignore_index=True)
                                
                                logging.debug(f'Processed: {file_path} >>> {sheet_name}')
                                
                                if len(processed_df_column) < len(full_column_list):
                                    logging.debug('Processed worksheet has been added to masterfile, however, there are missing columns')
                                
                            except:
                                logging.debug('Not processed:', file_path, sheet_name)
            
                ##########
                ##########
                # 4) if worksheet is without time reference, it will check if the sheet is either 'photo radar camera' or 'intersection safety device',
                # if so, these will be entered as derice type, and extract year and month, then standardize the column name, then merge into the master df.
                # If sheet name is not either, then it will search the the quarter reference from within the sheet, if exists, extract and rename column
                # if necessary and then merge into master df
                elif count_worksheet_with_name_without_time_ref >= 1:
                    for sheet_name, df in sheets_dict.items():     
                        # renew
                        curr_month = None
                        curr_quarter = None    
                        
                        if sheet_name in worksheet_without_time_ref:
                            logging.debug(f'Sheet name: {sheet_name}')
                            logging.debug(f'Worksheet without time reference on sheet name: {worksheet_without_time_ref}')

                            # renew                                
                            device_type_from_sheet_name = None
                            
                            # if worksheet name is "Photo Radar Camera"
                            if 'photo radar' in sheet_name.lower():
                                device_type_from_sheet_name = 'Photo Radar Camera'
                            
                            # if worksheet name is "Intersection Safety Device"
                            if 'intersection safety' in sheet_name.lower():
                                device_type_from_sheet_name = 'Intersection Safety Device'
                            
                            # If either "Photo Radar" or "Intersection Safety" substrings in the worksheet name
                            if device_type_from_sheet_name:
                                curr_device_type=sheet_name.title()
                            else:
                                curr_device_type = None

                            try:                          
                                # Process each worksheet at a time
                                processed_df = process_excel_worksheet_without_time_ref_in_sheetname_into_df(
                                    df, curr_year, 
                                    municipality=curr_municipality,
                                    device_type=curr_device_type)

                                # Only retain columns in processed_df that exist in df_master's column list
                                processed_df_column = processed_df.columns
                                valid_column = list(set(processed_df_column).intersection(full_column_list))
                                processed_df = processed_df[valid_column]

                                # Insert the individual df to the master_df
                                master_df = pd.concat([master_df, processed_df], ignore_index=True)
                                
                                logging.debug(f'Processed: {file_path} >>> {sheet_name}')
                                
                                if len(processed_df_column) < len(full_column_list):
                                    logging.debug('Processed worksheet has been added to masterfile, however, there are missing columns')

                            except:
                                logging.debug('Not processed:', file_path, sheet_name)

    # # clean up quarter column
    master_df['QUARTER'] = master_df['QUARTER'].apply(quarter_column_cleanup)

    # remove duplicate and format some data values
    dup_col_check = ['MUNICIPALITY', 'YEAR', 'MONTH', 'QUARTER', 'SITE_NAME', 'SITE_ID', 'DEVICE_TYPE', 'LOCATION_DESCRIPTION', 
                     'DATE_OF_LAST_ASSESSED', 'DEPLOYMENT_HOURS', 'NUMBER_OF_VEHICLES_MONITORED']
    master_df_dup_removed = master_df.drop_duplicates(subset=dup_col_check, keep='first')
    duplicated_rows = master_df[master_df.duplicated(subset=dup_col_check, keep=False)]

    # remove rows that are completely empty
    master_df = master_df.dropna(how='all')
    master_df_dup_removed = master_df_dup_removed.dropna(how='all')
    duplicated_rows = duplicated_rows.dropna(how='all')

    if save_csv_switch:
        resuilt_file_path = os.path.join(result_path, 'master_df.csv')        
        master_df_dup_removed.to_csv(resuilt_file_path, index=False)      
        
        # master_df.to_csv('test_master_df.csv', index=False)    
        # master_df_dup_removed.to_csv('test_master_df_no_dup.csv', index=False)
        # duplicated_rows.to_csv('test_master_df_only_dup.csv', index=False)

    logging.debug(f'master_df row n: {len(master_df)}')
    logging.debug(f'master_df_dup_removed row n: {len(master_df_dup_removed)}')
    logging.debug(f'duplicated_rows row n: {len(duplicated_rows)}')
    logging.debug('Finished...')

if __name__ == '__main__':
    main()