# Import libraries
import datetime
import glob
import pandas as pd
import onedigital_utilities as ut

# ---------------------------------------------------------------------------------
# PROCESSING STARTS HERE
# ---------------------------------------------------------------------------------

# Define configuration parameters.
# Base folder path where all of data is stored
base_path = "/Users/leocyriac/Wesfarmers-OneDigital/tables"

store_addresses_dict = {"BELMONT" : "Bunnings Notting Hill, 232 Ferntree Gully Rd, Notting Hill VIC 3168",
                   "GEELONG" : "Officeworks Geelong, 150 Malop St, Geelong VIC 3220",
                   "NOTTING HILL" : "Kmart Belmont, Belmont Ave, Belmont WA 6104"
                  }

location_lat_longs_dict = ut.get_lat_long_values_of_address(store_addresses_dict)

station_suburb_dict = ut.get_data_path_for_nearest_station(location_lat_longs_dict, base_path)


# List of suburbs and closest weather station's data
#station_suburb_dict = {"BELMONT": base_path + "/wa/perth_metro/*.csv",
#                       "GEELONG": base_path + "/vic/breakwater_(geelong_racecourse)/*.csv",
#                       "NOTTING HILL": base_path + "/vic/moorabbin_airport/*.csv"
#                       }
# Threshold temperature to compare against.
in_temperature = 35
in_num_years = 9

today = datetime.date.today()
curr_year = today.year

# printing the configurations used for debug purpose.
print("---------------------------------------------------------------------------------")
print("CONFIGURATIONS USED FOR THIS PROCESS")
print("---------------------------------------------------------------------------------")
print(f"Input temperature threshold: {in_temperature} ")

file_list = []
for locality, path in station_suburb_dict.items():
    # Combining all files to be processed into a list. These file names are stored along with their path.
    file_list = file_list + glob.glob(path)
    print(f"locality - {locality}, path - {path}")

print("---------------------------------------------------------------------------------")
print("Starting File validations")
print("---------------------------------------------------------------------------------")
# File validations starts here.
# Validation1: Checking if number of columns are same across all files.
if ut.check_column_count(file_list):
    print("Validation1: All files passed column count validation")
else:
    print("Validation1: Files have different count of columns")
    exit(1)

print("---------------------------------------------------------------------------------")

# Validation2: Checking if column names and column orders are same across all files.
if ut.check_column_names_match(file_list):
    print("Validation2: All files passed column name validation")
else:
    print("Validation2: Files have different column names or column order")
    exit(2)

print("---------------------------------------------------------------------------------")

# Validation3: Data reconciliation to make sure the files are not corrupted.
csv_exclude_list = ut.data_reconciliation(file_list)

if len(csv_exclude_list) == 0:
    print("Validation3: All files reconciled successfully")
else:
    print(f"Validation3: These files couldn't be reconciled and will be excluded from processing: {csv_exclude_list}")

print("---------------------------------------------------------------------------------")
print("Files validated successfully. Starting to process them")
print("---------------------------------------------------------------------------------")

# Now that validations completed successfully, file processing starts here.
# Initialising variables
bom_df = pd.DataFrame()
total_csv_files = 0

# Loop through each weather station folder to load the relevant CSV files into pandas dataframe.
for locality, path in station_suburb_dict.items():
    # Combining all files to be processed for the weather station into a list.
    # These file names are stored along with their path.
    csv_files = glob.glob(path)

    # Read each CSV file into DataFrame
    # This creates a list of dataframes for each weather station data
    df_list = (ut.get_df_from_csv(csv_file) for csv_file
               in csv_files)

    # Concatenate all DataFrames
    big_df = pd.concat(df_list, ignore_index=True)
    big_df["locality_name"] = locality
    bom_df = pd.concat([bom_df, big_df], ignore_index=True)
    total_csv_files = total_csv_files + len(csv_files)
    print(f"Locality: {locality}")
    print(f"No. of CSV files processed: {len(csv_files)}")
    print(f"Total Number of rows processed: {big_df.shape[0]}")
    print("---------------------------------------------------------------------------------")

print(f"Total number of CSV files processed: {total_csv_files}")
print(f"Total number of rows processed: {bom_df.shape[0]}")
print("---------------------------------------------------------------------------------")

# Renaming column names.
bom_df.columns = ['station_name',
                  'date',
                  'evapo_transpiration_0000-2400',
                  'rain_0900-0900',
                  'pan_evaporation_0900-0900',
                  'maximum_temperature',
                  'minimum_temperature',
                  'maximum_relative_humidity',
                  'minimum_relative_humidity',
                  'average_10m_wind_wpeed',
                  'solar_radiation',
                  'locality_name']

print(bom_df.info())
print("---------------------------------------------------------------------------------")

# Keeping a copy of the dataframe before the transformations for future use cases
bom_df_original = bom_df.copy()

# Dropping unwanted columns
bom_df.drop(['station_name',
             'evapo_transpiration_0000-2400',
             'rain_0900-0900',
             'pan_evaporation_0900-0900',
             'minimum_temperature',
             'maximum_relative_humidity',
             'minimum_relative_humidity',
             'average_10m_wind_wpeed',
             'solar_radiation'], axis=1, inplace=True)

# Data type conversions
# 1. Converting "date" field from string to date type.
#    Wrapping these in try/except to handle any unknown scenarios.
try:
    # Data analysis show that there are mixed formats in date field.
    # So, no format is specified while converting so that pandas will do the job if it's a valid format.
    bom_df["date"] = pd.to_datetime(bom_df["date"])
except:
    print("Failed to convert 'date' field from string to date")
    exit(3)

# 2. Converting "maximum_temperature" field from string to numeric type.
#    Wrapping these in try/except to handle any unknown scenarios.
try:
    # Data analysis show that there are missing values (or spaces) in maximum_temperature field
    # To deal with missing values errors='coerce' is used. This will fill those values can't be converted to NaN
    bom_df["maximum_temperature"] = pd.to_numeric(bom_df["maximum_temperature"], errors='coerce')
except:
    print("Failed to convert 'maximum_temperature' field from string to numeric")
    exit(4)

try:
    # Dropping all rows with missing values as we don't have required information.
    # Other options is to fill the missing values with the values from previous row.
    bom_df = bom_df.dropna()

    # Adding a new column "year" to do the aggregation based on year.
    bom_df['year'] = bom_df['date'].dt.year

    # Now that "year" column is added, we don't need "date" column anymore.
    bom_df.drop(['date'], axis=1, inplace=True)

    # Filter data as per problem statement.
    # ie only those with maximum temperature > 35 is required.
    # And only need the data for the past 9 years
    df2 = bom_df[(bom_df.maximum_temperature > in_temperature) & (bom_df.year > (curr_year - in_num_years))]

    # Finding the number of days in every year with this temperature.
    output_df = df2.groupby(by=['locality_name', 'year'], dropna=True).count()

    # Renaming the column with the count of temperature.
    output_df.columns = ["count"]

    # Group by has made columns 'locality_name', 'year' as indexes.
    # Using reset_index() these two is put back as columns and we will have our required dataframe.
    output_df = output_df.reset_index()
    output_df.to_csv('/Users/leocyriac/Wesfarmers-OneDigital/output/extreme_temperatures_yearly.csv', index=False)
except:
    print("Failed to get the final output")
    exit(4)

print(output_df)
