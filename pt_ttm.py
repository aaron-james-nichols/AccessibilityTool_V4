import os
import zipfile
from datetime import datetime, timedelta
import csv
import utm
import math
import osmnx as ox
import pandas as pd
import random

os.system('cls')

# # These are temporary inputs for testing purposes. Delete them later.
# input_lat = -37.80240195043516
# input_lon = 144.9634848512107
#
# weekday = 'tuesday'
# start_time = '14:30'
# max_travel_mins = 60
# transfers = True
# max_walk_mins = 10
# GTFS = ['Victoria/Bus.zip', 'Victoria/Tram.zip', 'Victoria/Metro.zip']

def accessed_stops(input_lat, input_lon, GTFS, transfers, start_time, weekday, max_travel_mins, max_walk_mins):

    transfer_dist = 100 # Meters

    utm_inputs = utm.from_latlon(input_lat, input_lon)

    utm_lat = utm_inputs[1]
    utm_lon = utm_inputs[0]

    closest_stop = {'stop_id':'stop_id', 'distance':math.inf}

    # THIS SECTION PREPARES AND OPENS ALL GTFS DATA.
    # Empty dataframes that GTFS data will be added to.
    stops_df = pd.DataFrame()
    calendar_df = pd.DataFrame()
    calendar_dates_df = pd.DataFrame()
    trips_df = pd.DataFrame()
    stop_times_df = pd.DataFrame()

    # Opens all of the relevant .txt files and combines them into single dataframes.
    for file in GTFS:
        zip_ref = zipfile.ZipFile(file, 'r')

        # Opens the stops.txt file as a dataframe.
        stops_txt = zip_ref.open('stops.txt')
        file_stops_df = pd.read_csv(stops_txt, dtype = {
        'stop_id':'str',
        'stop_lat':'float',
        'stop_lon':'float'
        })

        file_stops_df['file'] = str(file)
        stops_df = pd.concat([stops_df, file_stops_df], ignore_index = True)

        # Opens the calendar.txt file as a dataframe.
        calendar_txt = zip_ref.open('calendar.txt')
        file_calendar_df = pd.read_csv(calendar_txt, dtype = {
        'service_id':'str',
        'monday':'str',
        'tuesday':'str',
        'wednesday':'str',
        'thursday':'str',
        'friday':'str',
        'saturday':'str',
        'sunday':'str',
        'start_date':'int64',
        'end_date':'int64'
        })

        file_calendar_df['file'] = str(file)
        calendar_df = pd.concat([calendar_df, file_calendar_df], ignore_index = True)

        # Opens the calendar_dates.txt file as a dataframe.
        calendar_dates_txt = zip_ref.open('calendar_dates.txt')
        file_calendar_dates_df = pd.read_csv(calendar_dates_txt, dtype = {
        'service_id':'str',
        'date':'int64',
        'exception_type':'str'
        })

        file_calendar_dates_df['file'] = str(file)
        calendar_dates_df = pd.concat([calendar_dates_df, file_calendar_dates_df], ignore_index = True)

        # Opens the trips.txt file as a dataframe.
        trips_txt = zip_ref.open('trips.txt')
        file_trips_df = pd.read_csv(trips_txt, dtype = {
        'trip_id':'str',
        'service_id':'str'
        })

        file_trips_df['file'] = str(file)
        trips_df = pd.concat([trips_df, file_trips_df], ignore_index = True)

        # Opens the stop_times.txt file as a database.
        stop_times_txt = zip_ref.open('stop_times.txt')
        file_stop_times_df = pd.read_csv(stop_times_txt, dtype = {
        'stop_id':'str',
        'trip_id':'str',
        'arrival_time':'str',
        'departure_time':'str'
        })

        file_stop_times_df['file'] = str(file)
        stop_times_df = pd.concat([stop_times_df, file_stop_times_df], ignore_index = True)

    # THIS SECTION DETERMINES IF NEW IDS ARE REQUIRED, THEN MAKES THEM.
    # List of all stop IDs.
    stop_ids = stops_df['stop_id'].tolist()

    # Set of unique stop IDs.
    unique_ids = set(stop_ids)

    # Checks to see if there are duplicat IDs. If so, new IDs are created.
    new_ids = False
    if len(stop_ids) > len(unique_ids):

        new_ids = True

        # Sets up the dictionary of IDs.
        stop_id_unique = {}
        master_set = set()
        for file in GTFS:
            key = str(file)
            stop_id_unique[key] = {}

        # Adds values to the dictionary of IDs so that the old ID can be given and the new ID can be found.
        for file in GTFS:
            file_ids = stops_df[stops_df['file'] == str(file)]['stop_id'].tolist()
            for id in file_ids:
                new_id = str(random.randint(100000, 999999))
                while new_id in master_set:
                    new_id = str(random.randint(100000, 999999))
                master_set.add(new_id)
                stop_id_unique[str(file)][id] = new_id

        # Replaces the old stop IDs with the new, random stop IDs in the stops dataframe.
        for index, row in stops_df.iterrows():
            file = row['file']
            old_id = row['stop_id']
            stops_df.loc[index, 'stop_id'] = stop_id_unique[file][old_id]

    # PROCESSES THE STOPS DATAFRAME.
    # Gets the column names as a list.
    stops_df_columns = stops_df.columns.tolist()

    stops_dict = {}
    stop_name_dict = {}
    for index, row in stops_df.iterrows():
        stop_id = row['stop_id']
        stop_lat = row['stop_lat']
        stop_lon = row['stop_lon']
        stop_name = row['stop_name']
        if 'location_type' in stops_df_columns:
            location_type = row['location_type']
            if location_type == 0:
                stops_dict[stop_id] = {'stop_name':stop_name, 'stop_lat':stop_lat, 'stop_lon':stop_lon, 'stop_times':[]}
                stop_name_dict[stop_name] = []

                stop_utm = utm.from_latlon(stop_lat, stop_lon)
                stop_lat_utm = stop_utm[1]
                stop_lon_utm = stop_utm[0]

                distance = ox.distance.euclidean(utm_lat, utm_lon, stop_lat_utm, stop_lon_utm)
                if distance < closest_stop['distance']:
                    closest_stop['stop_id'] = stop_id
                    closest_stop['distance'] = distance

        else:
            stops_dict[stop_id] = {'stop_name':stop_name, 'stop_lat':stop_lat, 'stop_lon':stop_lon, 'stop_times':[]}
            stop_name_dict[stop_name] = []

            stop_utm = utm.from_latlon(stop_lat, stop_lon)
            stop_lat_utm = stop_utm[1]
            stop_lon_utm = stop_utm[0]

            distance = ox.distance.euclidean(utm_lat, utm_lon, stop_lat_utm, stop_lon_utm)
            if distance < closest_stop['distance']:
                closest_stop['stop_id'] = stop_id
                closest_stop['distance'] = distance

    for index, row in stops_df.iterrows():
        if 'location_type' in stops_df_columns:
            location_type = row['location_type']
            if location_type == 0:
                stop_id = row['stop_id']
                stop_name = row['stop_name']
                stop_name_dict[stop_name].append(stop_id)

        else:
            stop_id = row['stop_id']
            stop_name = row['stop_name']
            stop_name_dict[stop_name].append(stop_id)

    # Creates a dictionary of stop names with the middle point for each collection of stops associated with that name.
    names_list = list(stop_name_dict.keys())
    simple_stops = {}
    for name in names_list:
        lats = []
        lons = []
        for stop in stop_name_dict[name]:
            stop_lat = stops_dict[stop]['stop_lat']
            stop_lon = stops_dict[stop]['stop_lon']
            lats.append(stop_lat)
            lons.append(stop_lon)
        lat_max = max(lats)
        lat_min = min(lats)
        lon_max = max(lons)
        lon_min = min(lons)
        lat_mid = lat_min + ((lat_max - lat_min) / 2)
        lon_mid = lon_min + ((lon_max - lon_min) / 2)
        simple_stops[name] = {'lat_mid':lat_mid, 'lon_mid':lon_mid}

    start_point = closest_stop['stop_id']

    # Loops through the stops, adds the utm coordinates to the dataframe.
    for index, row in stops_df.iterrows():
        stop_lat = row['stop_lat']
        stop_lon = row['stop_lon']
        stop_utm = utm.from_latlon(stop_lat, stop_lon)
        utm_lat = stop_utm[1]
        utm_lon = stop_utm[0]
        stops_df.loc[index, 'utm_lat'] = utm_lat
        stops_df.loc[index, 'utm_lon'] = utm_lon

    # Loops thorugh the stops, for each stop, it calculates all distances to all other stops, adds the close ones to a dictionary.
    transfer_dict = {}
    for index, row in stops_df.iterrows():
        origin_id = row['stop_id']
        origin_utm_lat = row['utm_lat']
        origin_utm_lon = row['utm_lon']
        stops_df['distance'] = (abs(origin_utm_lat - stops_df['utm_lat'])**2 + abs(origin_utm_lon - stops_df['utm_lon'])**2)**0.5
        if 'location_type' in stops_df_columns:
            transfer_nodes = stops_df[(stops_df['distance'] <= transfer_dist) & (stops_df['location_type'] == 0)]['stop_id'].values
        else:
            transfer_nodes = stops_df[stops_df['distance'] <= transfer_dist]['stop_id'].values
        transfer_dict[origin_id] = transfer_nodes

    # THIS SECTION IDENTIFIES THE PREVIOUS DAY TO THE INPUT DAY.
    # Converts the start and end times to the number of minutes past midnight.
    orig_start_time_mins = (int(start_time[:2]) * 60) + int(start_time[3:])
    orig_end_time_mins = orig_start_time_mins + max_travel_mins

    # If the end time is after midnight, the end time is adjusted so that it starts counting at zero from midnight.
    if orig_end_time_mins >= 24 * 60:
        orig_end_time_mins = orig_end_tme_mins - (24 * 60)

    # Determines the preceding day to the input day.
    if weekday == 'monday':
        previous_day = 'sunday'
    elif weekday == 'tuesday':
        previous_day = 'monday'
    elif weekday == 'wednesday':
        previous_day = 'tuesday'
    elif weekday == 'thursday':
        previous_day = 'wednesday'
    elif weekday == 'friday':
        previous_day = 'thursday'
    elif weekday == 'saturday':
        previous_day = 'friday'
    elif weekday == 'sunday':
        previous_day = 'saturday'

    # THIS SECTION PROCESSES THE CALENDAR DATAFRAME.
    first_date = 30000101
    for index, row in calendar_df.iterrows():
        service_id = row['service_id']
        start_date = row['start_date']
        if start_date < first_date:
            first_date = start_date

    first_date = str(first_date)
    first_year = int(first_date[:4])
    first_month = int(first_date[4:6])
    first_day = int(first_date[6:])

    first_date_obj = datetime(first_year, first_month, first_day)
    start_weekday = datetime.weekday(first_date_obj)

    if start_weekday == 0:
        first_monday = first_date_obj
    elif start_weekday != 0:
        first_monday = first_date_obj + timedelta(days = 7 - start_weekday)

    # Dictionary of weekdays and the corresponding first dates in the GTFS dataset that.
    date_dict = {}

    monday = int(first_monday.strftime('%Y%m%d'))
    date_dict['monday'] = monday
    tuesday = int((first_monday + timedelta(days = 1)).strftime('%Y%m%d'))
    date_dict['tuesday'] = tuesday
    wednesday = int((first_monday + timedelta(days = 2)).strftime('%Y%m%d'))
    date_dict['wednesday'] = wednesday
    thursday = int((first_monday + timedelta(days = 3)).strftime('%Y%m%d'))
    date_dict['thursday'] = thursday
    friday = int((first_monday + timedelta(days = 4)).strftime('%Y%m%d'))
    date_dict['friday'] = friday
    saturday = int((first_monday + timedelta(days = 5)).strftime('%Y%m%d'))
    date_dict['saturday'] = saturday
    sunday = int((first_monday + timedelta(days = 6)).strftime('%Y%m%d'))
    date_dict['sunday'] = sunday

    calendar_dict = {}
    for index, row in calendar_df.iterrows():
        service_id = row['service_id']
        weekdays = list(date_dict.keys())
        weekday_dict = {}
        for weekday in weekdays:
            weekday_dict[date_dict[weekday]] = '0'
        calendar_dict[service_id] = weekday_dict

    for index, row in calendar_df.iterrows():
        service_id = row['service_id']
        start_date = row['start_date']
        end_date = row['end_date']
        if date_dict['monday'] >= start_date and date_dict['monday'] <= end_date:
            calendar_dict[service_id][date_dict['monday']] = row['monday']
        if date_dict['tuesday'] >= start_date and date_dict['tuesday'] <= end_date:
            calendar_dict[service_id][date_dict['tuesday']] = row['tuesday']
        if date_dict['wednesday'] >= start_date and date_dict['wednesday'] <= end_date:
            calendar_dict[service_id][date_dict['wednesday']] = row['wednesday']
        if date_dict['thursday'] >= start_date and date_dict['thursday'] <= end_date:
            calendar_dict[service_id][date_dict['thursday']] = row['thursday']
        if date_dict['friday'] >= start_date and date_dict['friday'] <= end_date:
            calendar_dict[service_id][date_dict['friday']] = row['friday']
        if date_dict['saturday'] >= start_date and date_dict['saturday'] <= end_date:
            calendar_dict[service_id][date_dict['saturday']] = row['saturday']
        if date_dict['sunday'] >= start_date and date_dict['sunday'] <= end_date:
            calendar_dict[service_id][date_dict['sunday']] = row['sunday']

    # THIS SECTION PROCESSES THE CALENDAR_DATES DATAFRAME.
    for index, row in calendar_dates_df.iterrows():
        service_id = row['service_id']
        date = row['date']
        exception_type = row['exception_type']
        if date >= date_dict['monday'] and date <= date_dict['sunday']:
            if exception_type == '1':
                calendar_dict[service_id][date] = '1'
            elif exception_type == '2':
                calendar_dict[service_id][date] = '0'

    # THIS SECTION PROCESSES THE TRIPS DATAFRAME.
    trips_dict = {}
    for index, row in trips_df.iterrows():
        trip_id = row['trip_id']
        service_id = row['service_id']
        trips_dict[trip_id] = {'service_id':service_id}

    # start_name = stops_dict[start_point]['stop_name']
    # start_points = stop_name_dict[start_name]

    # Identifies the start points that are within an acceptable distance of the closest stop.
    start_points = transfer_dict[start_point]

    # THIS SECTION PROCESSES THE STOP_TIMES DATAFRAME.
    # Gets the column names as a list.
    stop_times_df_columns = stop_times_df.columns.tolist()

    # Removes unnecessary data from the dataframe.
    acceptable_columns = ['trip_id', 'arrival_time', 'departure_time', 'stop_id', 'stop_sequence', 'file']
    for col_name in stop_times_df_columns:
        if col_name not in acceptable_columns:
            del stop_times_df[col_name]

    # Creates a list of unique trip IDs. This will be used to create the stop_times_dict.
    trip_ids = set(stop_times_df['trip_id'].tolist())

    # Creates a new dictionary with trip IDs as the key, then a list of stop times associated with the trip as the value.
    stop_times_dict = {}
    for trip_id in trip_ids:
        stop_times_dict[trip_id] = []

    # Converts the stop_times dataframe into a list of dictionaries. It's faster to loop through.
    stop_times = stop_times_df.to_dict(orient = 'records')

    for stop_time in stop_times:

        # Reaplaces the old stop_ids with the new stop_ids, if they were replaced.
        if new_ids == True:
            file = stop_time['file']
            old_id = stop_time['stop_id']
            stop_id = stop_id_unique[file][old_id]
        else:
            stop_id = stop_time['stop_id']

        trip_id = stop_time['trip_id']
        arrival_time = stop_time['arrival_time']
        departure_time = stop_time['departure_time']
        stop_sequence = stop_time['stop_sequence']

        # Adds the stop times to the trips-based stop time dictionary.
        stop_times_dict[trip_id].append({'stop_id':stop_id, 'arrival_time':arrival_time, 'departure_time':departure_time, 'stop_sequence':stop_sequence})
        # Adds the stop times to the stops dictionary.
        stops_dict[stop_id]['stop_times'].append({'trip_id':trip_id, 'arrival_time':arrival_time, 'departure_time':departure_time, 'stop_sequence':stop_sequence})

    # Adjusts the start_points list so that it is consistent to the output of the analysis.
    start_points_new = []
    for point in start_points:
        stop_lat = stops_dict[point]['stop_lat']
        stop_lon = stops_dict[point]['stop_lon']
        start_points_new.append({'stop_id':point, 'ttm_r':max_travel_mins, 'stop_lat':stop_lat, 'stop_lon':stop_lon})

    start_points = start_points_new

    # THE FOLLOWING SECTION IS THE MAIN ANALYSIS. EVERYTHING BEFORE WAS PREPARING THE GTFS DATA.
    reached_stops_set = set()
    ttm_r_dict = {}

    while len(start_points) > 0:

        potential_trips = []

        # The first part of this function loops through all of the stop times associated with each starting point, and determines if the trip associated with that stop_time is valid.
        for point in start_points:

            point_id = point['stop_id']
            travel_budget = point['ttm_r']
            used_time = max_travel_mins - travel_budget
            start_time_mins = orig_start_time_mins + used_time
            end_time_mins = orig_start_time_mins + travel_budget
            if end_time_mins >= 24 * 60:
                end_time_mins = end_time_mins - (24 * 60)

            for stop_time in stops_dict[point_id]['stop_times']:

                depart_change = 'n'

                trip_id = stop_time['trip_id']
                service_id = trips_dict[trip_id]['service_id']

                # Adjusts the departure time, if later than 23:59.
                departure_time = stop_time['departure_time']
                departure_hour = int(departure_time[:2])
                if departure_hour >= 24:
                    depart_change = 'y'
                    new_departure_hour = str(departure_hour - 24)
                    if len(new_departure_hour) < 2:
                        new_departure_hour = '0' + new_departure_hour
                    departure_time_mins = (int(new_departure_hour) * 60) + int(departure_time[3:5])
                else:
                    departure_time_mins = (departure_hour * 60) + int(departure_time[3:5])

                # Adjusts the arrival time, if later than 23:59.
                arrival_time = stop_time['arrival_time']
                arrival_hour = int(arrival_time[:2])
                if arrival_hour >= 24:
                    new_arrival_hour = str(arrival_hour - 24)
                    if len(new_arrival_hour) < 2:
                        new_arrival_hour = '0' + new_arrival_hour
                    arrival_time_mins = (int(new_arrival_hour) * 60) + int(arrival_time[3:5])
                else:
                    arrival_time_mins = (arrival_hour * 60) + int(arrival_time[3:5])

                # Checks to see if the departure and arrival times are valid with the time constraints.
                if departure_time_mins >= start_time_mins and arrival_time_mins <= end_time_mins:
                    if depart_change == 'n':
                        if calendar_dict[service_id][date_dict[weekday]] == '1':
                            remaining_budget = travel_budget - (departure_time_mins - start_time_mins)
                            potential_trips.append({'trip_id':trip_id, 'remaining_budget':remaining_budget})
                    elif depart_change == 'y':
                        if calendar_dict[service_id][date_dict[previous_day]] == '1':
                            if departure_time_mins - start_time_mins >= 0:
                                remaining_budget = travel_budget - (departure_time_mins - start_time_mins)
                            else:
                                remaining_budget = travel_budget - ((departure_time_mins + (24 * 60)) - start_time_mins)
                            potential_trips.append({'trip_id':trip_id, 'remaining_budget':remaining_budget})

        # This part of the function loops through the previously identified trips, then determines if the stop_times associated with those trips are valid.
        reached_stops = {}
        for trip in potential_trips:
            trip_id = trip['trip_id']
            remaining_budget = trip['remaining_budget']
            for stop_time in stop_times_dict[trip_id]:

                board_time_mins = start_time_mins + (travel_budget - remaining_budget)
                if board_time_mins >= 24 * 60:
                    board_time_mins = board_time_mins - (24 * 60)

                departure_time = stop_time['departure_time']
                departure_hour = int(departure_time[:2])
                if departure_hour >= 24:
                    new_departure_hour = str(departure_hour - 24)
                    if len(new_departure_hour) < 2:
                        new_departure_hour = '0' + new_departure_hour
                    departure_time_mins = (int(new_departure_hour) * 60) + int(departure_time[3:5])
                else:
                    departure_time_mins = (departure_hour * 60) + int(departure_time[3:5])

                arrival_time = stop_time['arrival_time']
                arrival_hour = int(arrival_time[:2])
                if arrival_hour >= 24:
                    new_arrival_hour = str(arrival_hour - 24)
                    if len(new_arrival_hour) < 2:
                        new_arrival_hour = '0' + new_arrival_hour
                    arrival_time_mins = (int(new_arrival_hour) * 60) + int(arrival_time[3:5])
                else:
                    arrival_time_mins = (arrival_hour * 60) + int(arrival_time[3:5])

                if departure_time_mins >= board_time_mins and arrival_time_mins <= end_time_mins:
                    stops_set = set(reached_stops.keys())
                    remain_travel_time = end_time_mins - arrival_time_mins
                    if stop_time['stop_id'] in stops_set:
                        if reached_stops[stop_time['stop_id']] < remain_travel_time:
                            reached_stops[stop_time['stop_id']] = remain_travel_time
                    else:
                        reached_stops[stop_time['stop_id']] = remain_travel_time

                    # adds the remaining time to a list with all remaining times associated with this stop name.
                    reach_name = stops_dict[stop_time['stop_id']]['stop_name']
                    if reach_name not in set(ttm_r_dict.keys()):
                        ttm_r_dict[reach_name] = []
                        ttm_r_dict[reach_name].append(remain_travel_time)
                    else:
                        ttm_r_dict[reach_name].append(remain_travel_time)

        # Loops through the previously identified stop times and adds them to the list for the next loop.
        start_points = []
        reached_stops_list = list(reached_stops.keys())
        for stop in reached_stops_list:
            stop_name = stops_dict[stop]['stop_name']
            stop_lat = stops_dict[stop]['stop_lat']
            stop_lon = stops_dict[stop]['stop_lon']
            remain_travel_time = reached_stops[stop]

            if stop not in reached_stops_set:
                reached_stops_set.add(stop)
                # reached_stops_dict_list.append({'stop_id':stop, 'ttm_r':remain_travel_time, 'stop_lat':stop_lat, 'stop_lon':stop_lon})
                start_points.append({'stop_id':stop, 'ttm_r':remain_travel_time, 'stop_lat':stop_lat, 'stop_lon':stop_lon})

            if transfers == True:
                transfer_stops = transfer_dict[stop]
                # transfer_stops = stop_name_dict[stop_name]
                for transfer in transfer_stops:

                    if transfer not in reached_stops_set:

                        reached_stops_set.add(transfer)
                        # reached_stops_dict_list.append({'stop_id':transfer, 'ttm_r':remain_travel_time, 'stop_lat':stops_dict[transfer]['stop_lat'], 'stop_lon':stops_dict[transfer]['stop_lon']})
                        start_points.append({'stop_id':transfer, 'ttm_r':remain_travel_time, 'stop_lat':stops_dict[transfer]['stop_lat'], 'stop_lon':stops_dict[transfer]['stop_lon']})

                        stop_name = stops_dict[transfer]['stop_name']
                        if stop_name not in set(ttm_r_dict.keys()):
                            ttm_r_dict[stop_name] = []
                            ttm_r_dict[stop_name].append(remain_travel_time)
                        else:
                            ttm_r_dict[stop_name].append(remain_travel_time)

    # THIS SECTION SIMPLIFIES THE OUTPUT OF THE MAIN WHILE LOOP.
    # Creates a more simplified version of the accessible stops. This has one point for each cluster of stops with the same stop name.
    simple_dict_list = []
    reached_names = set()
    for stop in reached_stops_set:
        stop_name = stops_dict[stop]['stop_name']
        ttm_r = max(ttm_r_dict[stop_name])
        if stop_name not in reached_names:
            lat_mid = simple_stops[stop_name]['lat_mid']
            lon_mid = simple_stops[stop_name]['lon_mid']
            if ttm_r < max_walk_mins:
                walk_mins = ttm_r
            elif ttm_r >= max_walk_mins:
                walk_mins = max_walk_mins
            simple_dict_list.append({'stop_name':stop_name,'walk_mins':walk_mins, 'stop_lat':lat_mid, 'stop_lon':lon_mid})
            reached_names.add(stop_name)

    return simple_dict_list

# stops_accessed = accessed_stops(input_lat, input_lon, GTFS, transfers, start_time, weekday, max_travel_mins, max_walk_mins)
#
# # Writes list of accessed stops to a csv.
# keys = stops_accessed[0].keys()
# with open('melbourne_stops_test_2.csv', 'w', newline = '') as output_file:
#     dict_writer = csv.DictWriter(output_file, keys)
#     dict_writer.writeheader()
#     dict_writer.writerows(stops_accessed)
