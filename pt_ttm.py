import os
import zipfile
from datetime import datetime, timedelta
import csv
import utm
import math
import osmnx as ox

# input_lat = 50.8417710777
# input_lon = 4.3018435055
#
# # Inputs
# weekday = 'tuesday'
# start_time = '14:30'
# max_travel_mins = 45
# start_point = closest_stop['stop_id']
# transfers = False
# max_walk_mins = 10

def accessed_stops(input_lat, input_lon, GTFS, transfers, start_time, weekday, max_travel_mins, max_walk_mins):

    utm_inputs = utm.from_latlon(input_lat, input_lon)

    utm_lat = utm_inputs[1]
    utm_lon = utm_inputs[0]

    closest_stop = {'stop_id':'stop_id', 'distance':math.inf}

    # Opens the GTFS zipfile.
    zip_ref = zipfile.ZipFile(GTFS, 'r')

    # Opens the stops.txt file and prepares a list of dictionaries.
    txt_file = zip_ref.open('stops.txt')
    lines = txt_file.read().decode('utf-8').splitlines()

    headers = lines[0].split(',')
    stops = []
    for line in lines[1:]:
        values = line.split(',')
        data_dict = dict(zip(headers, values))
        stops.append(data_dict)

    stops_dict = {}
    stop_name_dict = {}
    for stop in stops:
        stop_id = stop['stop_id']
        stop_lat = float(stop['stop_lat'])
        stop_lon = float(stop['stop_lon'])
        stop_name = stop['stop_name'].replace('"','')
        if stop['location_type'] == '0':
            stops_dict[stop_id] = {'stop_name':stop_name, 'stop_lat':stop_lat, 'stop_lon':stop_lon, 'stop_times':[]}
            stop_name_dict[stop_name] = []

            stop_utm = utm.from_latlon(stop_lat, stop_lon)
            stop_lat_utm = stop_utm[1]
            stop_lon_utm = stop_utm[0]

            distance = ox.distance.euclidean_dist_vec(utm_lat, utm_lon, stop_lat_utm, stop_lon_utm)
            if distance < closest_stop['distance']:
                closest_stop['stop_id'] = stop_id
                closest_stop['distance'] = distance

    for stop in stops:
        if stop['location_type'] == '0':
            stop_id = stop['stop_id']
            stop_name = stop['stop_name'].replace('"','')
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

    # # Inputs
    # weekday = 'tuesday'
    # start_time = '14:30'
    # max_travel_mins = 45
    # start_point = closest_stop['stop_id']
    # transfers = False
    # max_walk_mins = 10

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

    # # Opens the GTFS zipfile.
    # zip_ref = zipfile.ZipFile('Brussels.zip', 'r')

    # Opens the calendar.txt file and prepares a list of dicitonaries.
    txt_file = zip_ref.open('calendar.txt')
    lines = txt_file.read().decode('utf-8').splitlines()

    headers = lines[0].split(',')
    calendar = []
    for line in lines[1:]:
        values = line.split(',')
        data_dict = dict(zip(headers, values))
        calendar.append(data_dict)

    # Identifies the earliest whole week in the GTFS dataset.
    first_date = 30000101
    for service in calendar:
        service_id = service['service_id']
        start_date = int(service['start_date'])
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

    monday = first_monday.strftime('%Y%m%d')
    date_dict['monday'] = monday
    tuesday = (first_monday + timedelta(days = 1)).strftime('%Y%m%d')
    date_dict['tuesday'] = tuesday
    wednesday = (first_monday + timedelta(days = 2)).strftime('%Y%m%d')
    date_dict['wednesday'] = wednesday
    thursday = (first_monday + timedelta(days = 3)).strftime('%Y%m%d')
    date_dict['thursday'] = thursday
    friday = (first_monday + timedelta(days = 4)).strftime('%Y%m%d')
    date_dict['friday'] = friday
    saturday = (first_monday + timedelta(days = 5)).strftime('%Y%m%d')
    date_dict['saturday'] = saturday
    sunday = (first_monday + timedelta(days = 6)).strftime('%Y%m%d')
    date_dict['sunday'] = sunday

    calendar_dict = {}
    for service in calendar:
        service_id = service['service_id']
        weekdays = list(date_dict.keys())
        weekday_dict = {}
        for weekday in weekdays:
            weekday_dict[date_dict[weekday]] = '0'
        calendar_dict[service_id] = weekday_dict

    for service in calendar:
        service_id = service['service_id']
        start_date = service['start_date']
        end_date = service['end_date']
        if date_dict['monday'] >= start_date and date_dict['monday'] <= end_date:
            calendar_dict[service_id][date_dict['monday']] = service['monday']
        if date_dict['tuesday'] >= start_date and date_dict['tuesday'] <= end_date:
            calendar_dict[service_id][date_dict['tuesday']] = service['tuesday']
        if date_dict['wednesday'] >= start_date and date_dict['wednesday'] <= end_date:
            calendar_dict[service_id][date_dict['wednesday']] = service['wednesday']
        if date_dict['thursday'] >= start_date and date_dict['thursday'] <= end_date:
            calendar_dict[service_id][date_dict['thursday']] = service['thursday']
        if date_dict['friday'] >= start_date and date_dict['friday'] <= end_date:
            calendar_dict[service_id][date_dict['friday']] = service['friday']
        if date_dict['saturday'] >= start_date and date_dict['saturday'] <= end_date:
            calendar_dict[service_id][date_dict['saturday']] = service['saturday']
        if date_dict['sunday'] >= start_date and date_dict['sunday'] <= end_date:
            calendar_dict[service_id][date_dict['sunday']] = service['sunday']

    # Opens the calendar_dates.txt file and prepares a list of dicitonaries.
    txt_file = zip_ref.open('calendar_dates.txt')
    lines = txt_file.read().decode('utf-8').splitlines()

    headers = lines[0].split(',')
    calendar_dates = []
    for line in lines[1:]:
        values = line.split(',')
        data_dict = dict(zip(headers, values))
        calendar_dates.append(data_dict)

    for service in calendar_dates:
        service_id = service['service_id']
        date = service['date']
        exception_type = service['exception_type']
        if int(date) >= int(date_dict['monday']) and int(date) <= int(date_dict['sunday']):
            if exception_type == '1':
                calendar_dict[service_id][date] = '1'
            elif exception_type == '2':
                calendar_dict[service_id][date] = '0'

    # # Opens the stops.txt file and prepares a list of dictionaries.
    # txt_file = zip_ref.open('stops.txt')
    # lines = txt_file.read().decode('utf-8').splitlines()
    #
    # headers = lines[0].split(',')
    # stops = []
    # for line in lines[1:]:
    #     values = line.split(',')
    #     data_dict = dict(zip(headers, values))
    #     stops.append(data_dict)
    #
    # stops_dict = {}
    # stop_name_dict = {}
    # for stop in stops:
    #     stop_id = stop['stop_id']
    #     stop_lat = float(stop['stop_lat'])
    #     stop_lon = float(stop['stop_lon'])
    #     stop_name = stop['stop_name'].replace('"','')
    #     if stop['location_type'] == '0':
    #         stops_dict[stop_id] = {'stop_name':stop_name, 'stop_lat':stop_lat, 'stop_lon':stop_lon, 'stop_times':[]}
    #         stop_name_dict[stop_name] = []
    #
    # for stop in stops:
    #     if stop['location_type'] == '0':
    #         stop_id = stop['stop_id']
    #         stop_name = stop['stop_name'].replace('"','')
    #         stop_name_dict[stop_name].append(stop_id)
    #
    # # Creates a dictionary of stop names with the middle point for each collection of stops associated with that name.
    # names_list = list(stop_name_dict.keys())
    # simple_stops = {}
    # for name in names_list:
    #     lats = []
    #     lons = []
    #     for stop in stop_name_dict[name]:
    #         stop_lat = stops_dict[stop]['stop_lat']
    #         stop_lon = stops_dict[stop]['stop_lon']
    #         lats.append(stop_lat)
    #         lons.append(stop_lon)
    #     lat_max = max(lats)
    #     lat_min = min(lats)
    #     lon_max = max(lons)
    #     lon_min = min(lons)
    #     lat_mid = lat_min + ((lat_max - lat_min) / 2)
    #     lon_mid = lon_min + ((lon_max - lon_min) / 2)
    #     simple_stops[name] = {'lat_mid':lat_mid, 'lon_mid':lon_mid}

    # Opens the trips.txt file and prepares a list of dictionaries.
    txt_file = zip_ref.open('trips.txt')
    lines = txt_file.read().decode('utf-8').splitlines()

    headers = lines[0].split(',')
    trips = []
    for line in lines[1:]:
        values = line.split(',')
        data_dict = dict(zip(headers, values))
        trips.append(data_dict)

    trips_dict = {}
    for trip in trips:
        trip_id = trip['trip_id']
        service_id = trip['service_id']
        trips_dict[trip_id] = {'service_id':service_id}

    start_name = stops_dict[start_point]['stop_name']
    start_points = stop_name_dict[start_name]

    # Opens the stop_times.txt file and prepares a list of dictionaries.
    txt_file = zip_ref.open('stop_times.txt')
    lines = txt_file.read().decode('utf-8').splitlines()

    headers = lines[0].split(',')
    stop_times = []
    for line in lines[1:]:
        values = line.split(',')
        data_dict = dict(zip(headers, values))
        stop_times.append(data_dict)

    # Creates a new dictionary with trip IDs as the key, then a list of stop times associated with the trip as the value.
    stop_times_dict = {}
    for stop_time in stop_times:
        trip_id = stop_time['trip_id']
        stop_times_dict[trip_id] = []

    # Loops through the stop_times list and adds the stop times to the trip lists and to the stop lists.
    # The arrival and departure times are also adjusted if they are after midnight.
    for stop_time in stop_times:

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

    ###
    # The following section is the main analysis. Everything before was preparing the GTFS data.
    # reached_stops_dict_list = []
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

        reached_stops = {}
        # This part of the function loops through the previously identified trips, then determines if the stop_times associated with those trips are valid.
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

        start_points = []
        reached_stops_list = list(reached_stops.keys())
        # Loops through the previously identified stop times and adds them to the list for the next loop.
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
                transfer_stops = stop_name_dict[stop_name]
                for transfer in transfer_stops:

                    if transfer not in reached_stops_set:
                        reached_stops_set.add(transfer)
                        # reached_stops_dict_list.append({'stop_id':transfer, 'ttm_r':remain_travel_time, 'stop_lat':stops_dict[transfer]['stop_lat'], 'stop_lon':stops_dict[transfer]['stop_lon']})
                        start_points.append({'stop_id':transfer, 'ttm_r':remain_travel_time, 'stop_lat':stops_dict[transfer]['stop_lat'], 'stop_lon':stops_dict[transfer]['stop_lon']})
    ###
    # This is the end of the main function.

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

    # if len(simple_dict_list) > 0:
    #     # Writes list of simple stops to a csv.
    #     keys = simple_dict_list[0].keys()
    #     with open('stops_simple.csv', 'w', newline = '') as output_file:
    #         dict_writer = csv.DictWriter(output_file, keys)
    #         dict_writer.writeheader()
    #         dict_writer.writerows(simple_dict_list)
    #
    #
    #
    # else:
    #     print('No public transport services at this time.')

# # Writes list of accessed stops to a csv.
# keys = reached_stops_dict_list[0].keys()
# with open('reached_stops.csv', 'w', newline = '') as output_file:
#     dict_writer = csv.DictWriter(output_file, keys)
#     dict_writer.writeheader()
#     dict_writer.writerows(reached_stops_dict_list)
