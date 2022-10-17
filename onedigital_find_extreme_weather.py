# Import libraries
import glob
import pandas as pd


def check_column_count(file_list):
    """
    Verifying if column count is same across all csv files getting processed
    :param file_list: list of files passed in as parameter
    :return: True or False depending on if all files have same number of columns or not
    """

    first_time = True
    for file in file_list:
        df = get_df_from_csv(file)

        if first_time:
            column_count = df.shape[1]
            print(f"Checking if all files have a column count of {column_count}")
            first_time = False

        if column_count != df.shape[1]:
            return False

    return True


def check_column_names_match(file_list):
    """
    Verifying if the column names and order are same across all files
    :param file_list: list of files passed in as parameter
    :return: True or False depending on if all files have same number of columns or not
    """
    first_time = True
    for file in file_list:
        df = pd.read_csv(file, encoding='windows-1252', skiprows=11, skip_blank_lines=True, engine='python')

        if first_time:
            column_list = df.columns
            print("Checking if all files have same column names and order at row 12 of every file")
            print(f"Expected column names in file : {column_list}")
            first_time = False

            # print(type(column_list))
            # print(type(df.columns))

        if list(column_list) != list(df.columns):
            return False

    return True


def data_reconciliation(file_list):
    """
    Reconciling data using the columns Transpiration and Rain
    :param file_list: list of files passed in as parameter
    :return: List of files rejected due to reconciliation failed.
    """
    rejected_files_list = []
    for file in file_list:
        try:
            df = pd.read_csv(file,
                             encoding='windows-1252',
                             skiprows=12,
                             usecols=['(mm)', '(mm).1'],
                             skip_blank_lines=True,
                             engine='python')

            df['(mm)'] = pd.to_numeric(df['(mm)'], errors='coerce').fillna(0)
            df['(mm).1'] = pd.to_numeric(df['(mm).1'], errors='coerce').fillna(0)

            calculated_sum_transpiration = df.sum().round(2)[0]
            calculated_sum_rain = df.sum().round(2)[1]

            sum_transpiration = df.iloc[-1, 0]
            sum_rain = df.iloc[-1, 1]

            if (calculated_sum_transpiration - sum_transpiration) != sum_transpiration or \
                    (calculated_sum_rain - sum_rain) != sum_rain:
                rejected_files_list.append(file)
        except:
            print(f"File: {file}")
            print(f"calculated_sum_transpiration: {calculated_sum_transpiration}")
            print(f"sum_transpiration: {sum_transpiration}")
            print(f"calculated_sum_rain: {calculated_sum_rain}")
            print(f"sum_transpiration: {sum_rain}")
    return rejected_files_list


def get_df_from_csv(file_name):
    return pd.read_csv(file_name, encoding='windows-1252', skiprows=12, skipfooter=1, skip_blank_lines=True,
                       engine='python')


# Define configuration parameters.
base_path = "/Users/leocyriac/Wesfarmers-OneDigital/tables"
station_suburb_dict = {"BELMONT": base_path + "/wa/perth_metro/*.csv",
                       "GEELONG": base_path + "/vic/breakwater_(geelong_racecourse)/*.csv",
                       "NOTTING HILL": base_path + "/vic/moorabbin_airport/*.csv"
                       }
in_temperature = 35

print("---------------------------------------------------------------------------------")
print("CONFIGURATIONS USED FOR THIS PROCESS")
print("---------------------------------------------------------------------------------")
print(f"Input temperature threshold: {in_temperature} ")

file_list = []
for locality, path in station_suburb_dict.items():
    file_list = file_list + glob.glob(path)
    print(f"locality - {locality}, path - {path}")

print("---------------------------------------------------------------------------------")
# print(len(file_list))
# print(file_list)

if check_column_count(file_list):
    print("All files passed column count validation")
else:
    print("Files have different count of columns")
    exit(1)

print("---------------------------------------------------------------------------------")
if check_column_names_match(file_list):
    print("All files passed column name validation")
else:
    print("Files have different column names or column order")
    exit(2)

print("---------------------------------------------------------------------------------")
csv_exclude_list = data_reconciliation(file_list)

if len(csv_exclude_list) == 0:
    print("All files reconciled successfully")
else:
    print(f"These files couldn't be reconciled and will be excluded from processing: {csv_exclude_list}")

all_localities_df = pd.DataFrame()
for locality, path in station_suburb_dict.items():
    print("---------------------------------------------------------------------------------")
    print(f"Processing files for locality : {locality}")
    print("---------------------------------------------------------------------------------")

    csv_files = glob.glob(path)
    # Read each CSV file into DataFrame
    # This creates a list of dataframes
    df_list = (get_df_from_csv(csv_file) for csv_file
               in csv_files)
    # Concatenate all DataFrames
    big_df = pd.concat(df_list, ignore_index=True)
    big_df["locality_name"] = locality
    all_localities_df = pd.concat([all_localities_df, big_df], ignore_index=True)
    print(f"Locality: {locality}")
    print(f"No. of CSV files processed: {len(csv_files)}")
    print(f"Total Number of rows processed: {big_df.shape[0]}")

print("---------------------------------------------------------------------------------")
# print(all_localities_df.shape)
# all_localities_df.info()

bom_df = all_localities_df.copy()

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
# print(bom_df.head())
# bom_df.info()

bom_df_original = bom_df.copy()
# print(bom_df_original.head())

bom_df.drop(['station_name',
             'evapo_transpiration_0000-2400',
             'rain_0900-0900',
             'pan_evaporation_0900-0900',
             'minimum_temperature',
             'maximum_relative_humidity',
             'minimum_relative_humidity',
             'average_10m_wind_wpeed',
             'solar_radiation'], axis=1, inplace=True)
# bom_df.head(20)
# bom_df.info()

# missing values in maximum_temperature field
# bom_df["maximum_temperature"] = bom_df["maximum_temperature"].fillna(0)
# Mixed format in date field
# bom_df["date"] = pd.to_datetime(bom_df["date"], format='%d/%m/%Y')
bom_df["date"] = pd.to_datetime(bom_df["date"])
# to deal with missing values errors='coerce' is used. This will fill those can't be converted to NaN
bom_df["maximum_temperature"] = pd.to_numeric(bom_df["maximum_temperature"], errors='coerce')
bom_df = bom_df.dropna()
# bom_df.info()

bom_df[bom_df["maximum_temperature"].isna()]
bom_df["maximum_temperature"] = bom_df["maximum_temperature"].fillna(0)
bom_df[bom_df["maximum_temperature"] == 0]

bom_df['year'] = bom_df['date'].dt.year
bom_df.drop(['date'], axis=1, inplace=True)

df2 = bom_df[bom_df.maximum_temperature > in_temperature]

final_df = df2.groupby(by=['locality_name', 'year'], dropna=True).count()
final_df.columns = ["count"]

print(final_df.reset_index())
