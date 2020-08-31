"""
    This script service as a data extracter for all covid-19 records in Saudi Arabia 
    form the same source used by MOH covied-19 dashboard
"""
#imports
import json
import requests
from datetime import datetime, timedelta
import itertools
import copy
import xlsxwriter
import csv
import boto3
from botocore.exceptions import ClientError
import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

# records list
records = {
    "Cases": [],
    "Recoveries": [],
    "Mortalities": [],
    "Active": [],
    "Critical": [],
    "Tested": []
}
cumulative_records = {
    "Cases": [],
    "Recoveries": [],
    "Mortalities": [],
    "Active": [],
    "Critical": [],
    "Tested": []
}

date_total = {}

# file name to store records - same as dataset uid
file_name = 'da_bpuzuq'

# special events 
events = {
    "2020-03-02": "First case of COVID-19",
    "2020-03-04": "Umrah suspension",
    "2020-03-09": "flights suspended to number of countries",
    "2020-03-15": "International flights suspension for 14 days",
    "2020-03-16": "Gov / private suspension ",
    "2020-03-21": "Domestic flights suspension",
    "2020-03-32": "Curfew started for 21 days (6am -7 pm)",
    "2020-03-26": "Riyadh, Makkah and Madinah lockdown - curfew (6am - 3pm)",
    "2020-03-29": "Jeddah lockdown",
    "2020-03-30": "Makkah , Madinah 24 hours curfew",
    "2020-04-02": "Makkah lockdown",
    "2020-04-04": "Jeddah areas lockdown - 24 h curfew",
    "2020-04-06": "Riyadh, Dammam , Tabuk , Dahran, Hafuf, Jeddah, Taif, Qatif , Khobar24h curfew",
    "2020-04-25": "Partial lifting of curfew in all cities except Makkah",
    "2020-06-21": "Curfew lifted(all regions)"
}

# API details | Note: Max count allowed 2000 -- pagination needed
api = {
    "base_url": "https://services6.arcgis.com/bKYAIlQgwHslVRaK/arcgis/rest/services/",
    "data_count": 2000,
    "allow_pagination": True,
    "date_order": "asc",
    "record_type": {
        "Cases": {
            "url": "VWPlacesCasesHostedView/FeatureServer/0/query?f=json&where=1=1&outFields=PlaceName_EN,PlaceName_AR,RegionName_EN,RegionName_AR,Confirmed,Reportdt&orderByFields=Reportdt ",
            "field": "Confirmed"
        },
        "Recoveries": {
            "url": "VWPlacesCasesHostedView/FeatureServer/0/query?f=json&where=1=1&outFields=PlaceName_EN,PlaceName_AR,RegionName_EN,RegionName_AR,Recovered,Reportdt&orderByFields=Reportdt ",
            "field": "Recovered"
        },
        "Mortalities": {
            "url": "VWPlacesCasesHostedView/FeatureServer/0/query?f=json&where=1=1&outFields=PlaceName_EN,PlaceName_AR,RegionName_EN,RegionName_AR,Deaths,Reportdt&orderByFields=Reportdt ",
            "field": "Deaths"
        }, 
        "Critical": {
            "url": "DailyCriticalCases_ViewLayer/FeatureServer/0/query?f=json&where=1=1&outFields=After,Reportdt&orderByFields=Reportdt ",
            "field": "After"
        }, 
        "Tested": {
            "url": "DailyTestPerformance_ViewLayer/FeatureServer/0/query?f=json&where=1=1&outFields=DailyTest,ReportDate&orderByFields=ReportDate ",
            "field": "DailyTest"
        }
    }
}


def getRecords(record_type, page=0):
    """
        get records from source
    """
    global date_total

    # construct full url
    results_count = '&resultOffset={}&resultRecordCount={}'.format(api['data_count']*page, api['data_count'])
    full_url = '{}{}{}{}'.format(
        api['base_url'],
        api['record_type'][record_type]['url'],
        api['date_order'],
        results_count)

    # send request
    res = _requester(full_url)

    # construct records list
    for record in res['features']:
        record = record['attributes']

        # filter out records with no date
        if not ('Reportdt' in record or 'ReportDate' in record):
            continue

        # special case for Tested api, it returns 'ReportDate' instead of 'Reportdt'
        if record_type == 'Tested':
            record['Reportdt'] = record['ReportDate']

        _date = datetime.fromtimestamp(record['Reportdt']/1000).strftime('%Y-%m-%d')
        _event = events.get(_date, "")

        # collect total ases for a day
        if record_type == 'Cases' or record_type == 'Recoveries' or record_type == 'Mortalities':
            date_total[_date] = date_total.get(_date, 0) + record[api['record_type'][record_type]['field']]

        records[record_type].append({
            "date": _date,
            "daily_cumulative": 'Cumulative' if record_type == 'Critical' else "Daily",
            "city_en": record['PlaceName_EN'].title() if 'PlaceName_EN' in record else "Total",
            "city_ar": record['PlaceName_AR'] if 'PlaceName_AR' in record else "الكل",
            "region_en": record['RegionName_EN'].title() if 'RegionName_EN' in record else "Total",
            "region_ar": record['RegionName_AR'] if 'RegionName_AR' in record else "الكل",
            "indicator": record_type,
            "case_value": record[api['record_type'][record_type]['field']],
            "event": _event
        })

    # check if there is more data go grap it
    if api['allow_pagination'] and len(res['features']) == api['data_count']:
        page += 1
        return getRecords(record_type, page)
    else:
        # calculate total cases for a day
        for day in date_total:
            records[record_type].append({
                "date": day,
                "daily_cumulative": "Daily",
                "city_en": "Total",
                "city_ar": "الكل",
                "region_en": "Total",
                "region_ar": "الكل",
                "indicator": record_type,
                "case_value": date_total[day],
                "event": events.get(day, '')
            })
        date_total = {}


def calculateActiveCases():
    """
        calculate daily Active from cumulative cases
        daily_Active = cumulative_Cases - cumulative_Recoveries - cumulative_Mortalities
    """
    city_day = {}
    for indicator_record in cumulative_records:
        if indicator_record == 'Cases' or indicator_record == 'Recoveries' or indicator_record == 'Mortalities':
            # create a factor to multiply by
            if indicator_record == 'Cases':
                factor = 1
            else:
                factor = -1

            for record in cumulative_records[indicator_record]:
                _key = '{}_{}'.format(record['city_en'], record['date'])
                _temp = copy.deepcopy(record)
                if _key not in city_day:
                    _temp['case_value'] *= factor
                    _temp['indicator'] = "Active"
                    city_day[_key] = _temp
                else:
                    city_day[_key]['case_value'] += _temp['case_value'] * factor

    # store daily records
    for record in city_day.values():
        records['Active'].append(record)


def accumulate(types):
    """
        Daily accumulate records for each indicator & city (if applicable)
    """
    for indicator_record in records:
        if indicator_record not in types:
            continue

        accumulated_city = {}

        for record in records[indicator_record]:
            if record['city_en'] not in accumulated_city:
                accumulated_city[record['city_en']] = {
                    "value": record['case_value'],
                    "date": record['date']
                }
            else:
                cumulative_records[indicator_record] += fillMissingCumulativeDates(accumulated_city[record['city_en']], record)

                accumulated_city[record['city_en']]['value'] += record['case_value']
                accumulated_city[record['city_en']]['date'] = record['date']

            
            temp_record = copy.deepcopy(record)
            temp_record['daily_cumulative'] = "Cumulative"
            temp_record['case_value'] = accumulated_city[record['city_en']]['value']
            cumulative_records[indicator_record].append(temp_record)


def fillMissingCumulativeDates(last_record, next_record):
    p_date = datetime.strptime(last_record['date'], '%Y-%m-%d')
    n_date = datetime.strptime(next_record['date'], '%Y-%m-%d')
    diff_date = (n_date - p_date).days

    if diff_date < 2:
        return []
    
    days = []
    for i in range(1, diff_date):
        days.append({
            **next_record,
            "daily_cumulative": "Cumulative",
            "date": datetime.strftime(p_date+timedelta(days=i), '%Y-%m-%d'),
            "case_value": last_record['value']
        })
    return days


def _requester(url):
    r = requests.get(url)
    # this API will 200 for all requests even for 400 and 404 status codes
    # therefor, we are only returning when 200 and the json respnse contains ['features']
    if 'features' in r.json():
        return r.json()
    else:
        # stop all the process
        raise SystemExit("HTTP Error: {}".format(r))


def writeBulkToExcel():
    # create and setup work sheet
    workbook = xlsxwriter.Workbook(file_name + '.xlsx')
    worksheet = workbook.add_worksheet()

    # adding columns headers
    columns = ["date",
               "daily_cumulative",
               "city_en",
               "city_ar",
               "region_en",
               "region_ar",
               "indicator",
               "case_value",
               "event"]

    # write column_name in the first row
    for column_name in columns:
        col = columns.index(column_name)  # order.
        worksheet.write(0, col, column_name)

    # write bulk records
    row = 1
    for indicator_record in records:
        for record in records[indicator_record]:
            for _key, _value in record.items():
                col = columns.index(_key)
                worksheet.write(row, col, _value)
            row += 1  # go next row

        for record in cumulative_records[indicator_record]:
            for _key, _value in record.items():
                col = columns.index(_key)
                worksheet.write(row, col, _value)
            row += 1  # go next row

    workbook.close()


def writeCSV():
    columns = ["date",
               "daily_cumulative",
               "city_en",
               "city_ar",
               "region_en",
               "region_ar",
               "indicator",
               "case_value",
               "event"]

    with open(file_name + '.csv', 'w', encoding='utf8', newline='') as output_file:
        fc = csv.DictWriter(output_file, fieldnames=columns)
        fc.writeheader()
        for indicator_record in records:
            fc.writerows(records[indicator_record])
            fc.writerows(cumulative_records[indicator_record])


def upload_to_aws():
    ACCESS_KEY = os.environ.get('aws_access_key_id')
    SECRET_KEY = os.environ.get('aws_secret_access_key')
    BUCKET = os.environ.get('s3_bucket')

    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    file_to_upload = file_name + '.csv'

    try:
        s3.upload_file('./'+file_to_upload, BUCKET, file_to_upload, ExtraArgs={'ACL': 'public-read'})
        print("Done ... Upload Successful")
        return True
    except ClientError as e:
        print(e)
        return False


if __name__ == '__main__':
    getRecords('Cases')
    getRecords('Recoveries')
    getRecords('Mortalities')
    getRecords('Critical')
    getRecords('Tested')
    accumulate(["Cases", "Recoveries", "Mortalities", "Tested"])
    calculateActiveCases()
    # writeBulkToExcel()
    writeCSV()
    upload_to_aws()
