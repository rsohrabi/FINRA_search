#!/usr/bin/python
import urllib.request
import signal
import json
import sys
import io
import csv

try:
    to_unicode = unicode
except NameError:
    to_unicode = str

from pprint import pprint

''' FINRA User Verification script'''
# sample json data
sample_data_json = {'a list': [1, 42, 3.124, 'help', u'$'],
                    'a string': 'blah blah',
                    'another dict': {'foo': 'bar',
                                     'key': 'value',
                                     'the anser': 69}}

FRONT_25_HTTP = 25
BACK_2_HTTP = -2
FIRST_POSITION = 0
LAST_POSITION = 1
ZIP_POSITION = 2

def main():
    # get command line args
    if len(sys.argv) != 2:
        usage()
        sys.exit(1)

    csv_input_file = sys.argv[1]
    if(csv_input_file.endswith('.csv') is False):
        print("Expecting .csv file")
        usage()
        sys.exit(-1)
    csv_out_file = r'sample_input\outfile.csv'

    # todo: open log file for logging
    # todo: open output CSV file
    report_user_status = {}
    with open(csv_input_file, 'rt') as csv_data_file:
        csvReader = csv.reader(csv_data_file, dialect='excel')
        #drop the first row, as it is the tital
        next(csvReader, None)  # skip the headers
        for csv_row_count, row in enumerate(csvReader):
            search_info = {}

            first_name = row[FIRST_POSITION].strip()
            last_name = row[LAST_POSITION].strip()
            zipcode = row[ZIP_POSITION].strip()
            first_and_last = first_name + ' ' + last_name
            # process each entry
            print('first and last = %s zipcode %s' % (first_and_last, zipcode))
            search_all_user_matches(search_info, csv_row_count, first_name, last_name, zipcode)
            # if the search_info has value, then push to the output csv file
            with open('csv_out_file.csv', 'w') as csv_out_file:
                wr = csv.writer(csv_out_file, dialect='excel-tab')
                if (len(search_info)):
                    print("FOUND")
                    data = [first_name, last_name, zipcode, 'FOUND']
                else:
                    data = [first_name, last_name, zipcode, 'NOTFOUND']
                    print("NOTFOUND")
                #for item in data:
                #    wr.writerow([item],)
                wr.writerow([data],)

            #URL_string = 'https://doppler.finra.org/doppler-lookup/api/v1/search/individuals?hl=true&includePrevious=true&json.wrf=angular.callbacks._j&nrows=12&query=%s&r=25&sort=&wt=json' % (first_and_last_url)
            #print('URL is: %s' % (URL_string))
            # Send GET request to server
            #'https://doppler.finra.org/doppler-lookup/api/v1/search/individuals?hl=true&includePrevious=true&json.wrf=angular.callbacks._j&lat=40.734332&lon=-74.010112&nrows=12&query=edward+Bennett&r=25&sort=&wt=json')
            #response = urllib.request.urlopen(URL_string)

            #line = response.read()
            #line = line.decode('utf-8')
            # print(line)
            #fmt_line = clean_json_string(line, 25, -2)
            #print(fmt_line)
            #exit(1)
            # todo: Read output from request
            # content_json = response.read()
            #json_data = json.loads(fmt_line)
            # data = read_json_file(r'no_results.json')
            #pprint(json_data, indent=2)
            #process JSON retruned
            #json_data = read_json_file(r'json\Edward_Bennett.json')
            #pprint(json_data)


    # todo: open CSV input file

    exit(1)

    # todo: Send GET request to server



# pprint(content_json, indent=4)
# print(json.dumps(json_data, indent=4, sort_keys=True, default=str))
# print(to_string)
# error_code = content_json['errorCode']
# error_msg = content_json['errorMessage']
# print('error_code: %s error_message %s' % (error_code, error_msg))
#
# todo: catagorize entry and put in out CSV
# close all entry
def search_all_user_matches(user_info, csv_row_count, first, last, zipcode):
    #do first search results
    start_row = 0
    found_item = {}
    search_results = {}
    current_count = get_first_search_results(search_results, found_item, first, last, zipcode, start_row)
    # if search user is in this batch, then return. else keep looking with subsequent searches.
    if(user_found_in_results(search_results, found_item, first, last, zipcode) is False):
        clear_unwanted_users(search_results)
        # if there is more results ( based on the totalresults returned, get subsequent results.
        start_row += current_count
        while search_results['total_search_to_process'] > search_results['total_so_far']:
            current_count = get_subsequent_search_results(search_results, found_item, first, last, zipcode, start_row)
            start_row += current_count
            if (user_found_in_results(search_results, found_item, first, last, zipcode) is True):
                user_info.update(found_item)
                return
        clear_unwanted_users(search_results)
    else:
        # copy results into user_info for writing to csv file
        del(search_results)
        user_info.update(found_item)
    return


def get_first_search_results(search_results, found_item, first, last, zipcode, start_row):
    ''' this is the first query to the server'''
    first_and_last_url = first + '+' + last
    URL_string = 'https://doppler.finra.org/doppler-lookup/api/v1/search/individuals?hl=true&includePrevious=true&json.wrf=angular.callbacks._j&nrows=12&query=%s&r=25&sort=&wt=json' % (
    first_and_last_url)
    #URL_string = 'https://doppler.finra.org/doppler-lookup/api/v1/search/individuals?hl=true&includePrevious=true&json.wrf=angular.callbacks._j&nrows=12&query=%s&r=25&sort=&start=%s&wt=json' % (first_and_last_url,12)
    print('URL is: %s' % (URL_string))
    #Send GET request to server
    # 'https://doppler.finra.org/doppler-lookup/api/v1/search/individuals?hl=true&includePrevious=true&json.wrf=angular.callbacks._j&lat=40.734332&lon=-74.010112&nrows=12&query=edward+Bennett&r=25&sort=&wt=json')
    response = urllib.request.urlopen(URL_string)

    line = response.read()
    line = line.decode('utf-8')
    # print(line)
    fmt_line = clean_json_string(line, 25, -2)
    json_data = json.loads(fmt_line)
    #data = read_json_data(json_data)
    # pprint(json_data, indent=2)
    # process JSON retruned
    print('json error code: %s' % (json_data['errorCode']) )
    if json_data['errorCode'] != 0:
        print('request returned error with : %s' % json_data['errorMessage'])
    total_so_far = search_results['total_so_far'] = 0
    total_search_to_process = search_results['total_search_to_process'] = int(json_data['results']['BROKER_CHECK_REP']['totalResults'])
    print('total_search_results: %d' % (search_results['total_search_to_process']))

    current_search_processed = parse_search_json(json_data, search_results, total_so_far, first, last, zipcode)

    return current_search_processed


def user_found_in_results(search_results, found_item, first, last, zipcode):

    count = search_results['return_count']
    i=0
    while i < count:
        target = list(search_results[i])
        #print('target is %s' % target)
        if((target[0] == first.upper()) and target[2] == last.upper()):
            print('FOUND IT!')
            found_item['first_name'] = target[0]
            found_item['middle_name'] = target[1]
            found_item['last_name'] = target[2]
            found_item['zipcode'] = target[3]
            return True

        i = i + 1
    return False



def get_subsequent_search_results(search_results, found_item, first, last, zipcode, start_row):
    ''' this is the first query to the server'''
    first_and_last_url = first + '+' + last
    #URL_string = 'https://doppler.finra.org/doppler-lookup/api/v1/search/individuals?hl=true&includePrevious=true&json.wrf=angular.callbacks._j&nrows=12&query=%s&r=25&sort=&wt=json' % (
    #first_and_last_url)
    URL_string = 'https://doppler.finra.org/doppler-lookup/api/v1/search/individuals?hl=true&includePrevious=true&json.wrf=angular.callbacks._j&nrows=12&query=%s&r=25&sort=&start=%s&wt=json' % (first_and_last_url,start_row)
    print('URL is: %s' % (URL_string))
    #Send GET request to server
    # 'https://doppler.finra.org/doppler-lookup/api/v1/search/individuals?hl=true&includePrevious=true&json.wrf=angular.callbacks._j&lat=40.734332&lon=-74.010112&nrows=12&query=edward+Bennett&r=25&sort=&wt=json')
    response = urllib.request.urlopen(URL_string)

    line = response.read()
    line = line.decode('utf-8')
    # print(line)
    fmt_line = clean_json_string(line, 25, -2)
    json_data = json.loads(fmt_line)
    #data = read_json_data(json_data)
    # pprint(json_data, indent=2)
    # process JSON retruned
    if json_data['errorCode'] != 0:
        print('request returned error with : %s' % json_data['errorMessage'])
    total_in_this_query = search_results['total_so_far']
    #total_search_to_process = search_results['total_search_to_process']
    print('total_search_results: %d' % (total_in_this_query))

    current_search_processed = parse_search_json(json_data, search_results, total_in_this_query, first, last, zipcode)

    return current_search_processed

def parse_search_json(json_data, search_results, total_so_far, first, last, zipcode):
    ''' Search the json object for user
    return: 'validate' if found or 'not found'
    '''
    i = 0
    try:
        results_count = len(json_data['results']['BROKER_CHECK_REP']['results'])
    except (ValueError, KeyError, TypeError):
        results_count = 0
    search_results['return_count'] = results_count
    while (i < results_count):
        try:
            bc_first = json_data['results']['BROKER_CHECK_REP']['results'][i]['fields']['bc_firstname']
        except (ValueError, KeyError, TypeError):
            bc_first = 'none'
        try:
            bc_last = json_data['results']['BROKER_CHECK_REP']['results'][i]['fields']['bc_lastname']
        except (ValueError, KeyError, TypeError):
            bc_last = 'none'
        try:
            bc_middle = json_data['results']['BROKER_CHECK_REP']['results'][i]['fields']['bc_middlename']
        except (ValueError, KeyError, TypeError):
            bc_middle = 'none'
        try:
            bc_zip = json_data['results']['BROKER_CHECK_REP']['results'][i]['fields']['bc_current_employments'][0]['bc_branch_zip']
        except (ValueError, KeyError, TypeError):
            bc_zip = '11009'
        search_results.update({i: [bc_first, bc_middle, bc_last, bc_zip]})
        #pprint(search_results[total_so_far])
        total_so_far = total_so_far + 1
        search_results['total_so_far'] = total_so_far

        i= i+1

    return results_count


def clear_unwanted_users(data):
    data['return_count'] = 0
    for i in range(12):
        del(data[i])


def clean_json_string(data, begin_index, end_index):
    ''' clean up stream that has corrupt json.
    FINRA web site returns additional information in the GET return.
    Strip away the unneeded characters.
    NOTE:  This is hard coded index sizes based on current return values.
    '''
    json_raw = data
    json_raw = json_raw[begin_index:]
    json_raw = json_raw[:end_index]
    return json_raw


def read_json_file(file_name):
    with open(file_name) as in_file:
        data_loaded = json.load(in_file)

    return data_loaded


def write_json_file(file_name, data):
    with io.open(file_name, 'w', encoding='utf8') as out_file:
        json_thing = json.dumps(data, indent=4,
                                sort_keys=True, seperators=(',', ':'),
                                ensure_ascii=False)
        out_file.write(to_unicode(json_thing))


def usage():
    print(' -------------------------------------------------------------------------')
    print(' FINRA_checker ')
    print(' ')
    print(' Takes a .csv file with firstname, lastname, and zipcode.')
    print(' Searches the https://brokercheck.finra.org web site for match.')
    print(' Generate out_name_input_file.csv file with results:')
    print('       FOUND is when user was found')
    print('       NOTFOUND is when no match is found')
    print(' ')
    print(' Typical usage:')
    print(' FINRA_checker.py name_input_file.csv')
    print(' ')
    print(' -------------------------------------------------------------------------')
    sys.exit(' ')


class GracefulInterruptHandler(object):
    ''' Trape common signals for this script
    use [with GracefulInterruptHandler() as h:] as context manager
    to trap signals.  Can be nested multiple times.
    '''
    pass


if __name__ == '__main__':
    sys.exit(main())