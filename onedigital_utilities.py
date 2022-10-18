# Import libraries
import pandas as pd
from geopy.geocoders import ArcGIS
from geopy.distance import geodesic


def check_column_count(file_list):
    """
    Verifying if column count is same across all csv files getting processed
    :param file_list: list of files passed in as parameter
    :return: True or False depending on if all files have same number of columns or not
    """

    first_time = True
    for file in file_list:
        df = get_df_from_csv(file)

        # Store the column count of the first file so that it can be compared with other files.
        if first_time:
            column_count = df.shape[1]
            print(f"Checking if all files have a column count of {column_count}")
            first_time = False

        # if the stored column count is not matching with the current file, then return False.
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

        # Load the CSV file to a pandas dataframe.
        df = pd.read_csv(file,
                         encoding='windows-1252',
                         skiprows=11,
                         skip_blank_lines=True,
                         engine='python')

        # Store the column names of the first file so that it can be compared with other files.
        if first_time:
            column_list = df.columns
            print("Checking if all files have same column names and order at row 12 of every file")
            print(f"Expected column names in file : {column_list}")
            first_time = False

        # if the stored column names are not matching with the current file, then return False.
        # converting to list so that they can be compared
        if list(column_list) != list(df.columns):
            return False

    return True


def data_reconciliation(file_list):
    """
    Reconciling data using the columns Transpiration and Rain
    :param file_list: list of files passed in as parameter
    :return: List of files rejected due to failed reconciliation.
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
    """
    This function loads a csv file into pandas dataframe and returns the dataframe
    :param file_name: Input CSV file
    :return: Output pandas dataframe
    """
    return pd.read_csv(file_name,
                       encoding='windows-1252',
                       skiprows=12,
                       skipfooter=1,
                       skip_blank_lines=True,
                       engine='python')


def get_lat_long_values_of_address(addresses_dict):
    """
    Finds the latitude and longitude of list of addresses passed through the input dictionary.
    :param addresses_dict: input dictionary with key = suburb and value = store address
    :return: a dictionary with key = suburb and value = location (contains latitude and longitude)
    """
    # initialise the dictionary to be returned back with location details
    address_lat_longs_dict = {}

    # Create an ArcGIS object
    geolocator = ArcGIS()
    for suburb, address in addresses_dict.items():
        # get the location details of the address
        location = geolocator.geocode(address)
        address_lat_longs_dict[suburb] = location

    return address_lat_longs_dict


def get_data_path_for_nearest_station(address_lat_longs_dict, data_base_path):
    """
    This function,
    1. calculates the distance between each store and all weather stations and
    2. gets the weather station within least distance from the store
    3. finds out the data path for each of those weather stations
    4. finally creates a dictionary with key = suburb and value = dataset path for the related weather station

    :param address_lat_longs_dict: input dictionary with key = suburb and value = location (lat/long data)
    :param data_base_path: input basepath where all of BOM weather data is stored
    :return: output dictionary with key = suburb and value = dataset path for the related weather station
    """

    # Initialise the weather station file name along with full path
    stations_file = data_base_path + "/stations_db.txt"

    # The file with weather station data has the following properties,
    # 1. No headers present
    # 2. It's a fixed width file
    # 3. Last two fields are latitude and longitude
    cols = ['id1', 'state', 'id2', 'station_name', 'some_date', 'latitude', 'longitude']
    df_stations = pd.read_fwf(stations_file,
                              header=None,
                              widths=[8, 4, 6, 41, 16, 9, 10],
                              names=cols)

    # Combining the lat/long of the station to a single column so that the distance function can use this directly
    df_stations['station_lat_long'] = df_stations['latitude'].astype(str) + ',' + df_stations['longitude'].astype(str)

    # Initialising the output dictionary
    station_suburb_dict = {}
    for suburb, lat_long in address_lat_longs_dict.items():
        # getting the lat/long details for each store
        coords_1 = (lat_long.latitude, lat_long.longitude)

        # finding distance between the store and weather station.
        # This data is stored as a separate column (one for each store) in the weather station data.
        # split() is used to remove the unit ("km") from the calculated distance
        df_stations[suburb.lower() + '_dist'] = df_stations["station_lat_long"].apply(
            lambda coords_2: str(geodesic(coords_1, coords_2)).split()[0])

        # Converting the distance data to numeric so that arithmetic operation can be done.
        df_stations[suburb.lower() + '_dist'] = pd.to_numeric(df_stations[suburb.lower() + '_dist'])

        # Find out the row with minimum distance in the _dist column
        min_dist_df = df_stations[df_stations[suburb.lower() + '_dist'] == df_stations[suburb.lower() + '_dist'].min()]

        # Find out the data path for the weather station.
        # For this state and station name should be identified and transformed to the directory/path naming followed
        state = min_dist_df.iloc[0, 1].lower()
        station_name = min_dist_df.iloc[0, 3].lower().replace(" ", "_")

        # Last bit is to add to the output dictionary.
        station_suburb_dict[suburb] = data_base_path + "/" + state + "/" + station_name + "/*.csv"

    return station_suburb_dict
