"""
    This script service as a data extracter for all covid-19 records in Saudi Arabia 
    form the same source used by MOH covied-19 dashboard
"""
#imports
import json
import requests
from datetime import datetime
import itertools
import copy
import xlsxwriter

# records list
records = {
    "confirmed": [],
    "recovery": [],
    "death": [],
    "active": [],
    "critical": [],
    "tested": []
}
cumulative_records = {
    "confirmed": [],
    "recovery": [],
    "death": [],
    "active": [],
    "critical": [],
    "tested": []
}
# file name to store records
file_name = 'records.xlsx'

# API details | Note: Max count allowed 2000 -- pagination needed
api = {
    "base_url": "https://services6.arcgis.com/bKYAIlQgwHslVRaK/arcgis/rest/services/",
    "data_count": 2000,
    "allow_pagination": True,
    "date_order": "asc",
    "record_type": {
        "confirmed": {
            "url": "VWPlacesCasesHostedView/FeatureServer/0/query?f=json&where=1=1&outFields=PlaceName_EN,PlaceName_AR,RegionName_EN,RegionName_AR,Confirmed,Reportdt&orderByFields=Reportdt ",
            "field": "Confirmed"
        },
        "recovery": {
            "url": "VWPlacesCasesHostedView/FeatureServer/0/query?f=json&where=1=1&outFields=PlaceName_EN,PlaceName_AR,RegionName_EN,RegionName_AR,Recovered,Reportdt&orderByFields=Reportdt ",
            "field": "Recovered"
        },
        "death": {
            "url": "VWPlacesCasesHostedView/FeatureServer/0/query?f=json&where=1=1&outFields=PlaceName_EN,PlaceName_AR,RegionName_EN,RegionName_AR,Deaths,Reportdt&orderByFields=Reportdt ",
            "field": "Deaths"
        }, 
        "critical": {
            "url": "DailyCriticalCases_ViewLayer/FeatureServer/0/query?f=json&where=1=1&outFields=After,Reportdt&orderByFields=Reportdt ",
            "field": "After"
        }, 
        "tested": {
            "url": "DailyTestPerformance_ViewLayer/FeatureServer/0/query?f=json&where=1=1&outFields=DailyTest,ReportDate&orderByFields=ReportDate ",
            "field": "DailyTest"
        }
    }
}


def getRecords(record_type, page=0):
    """
        get records from source
    """
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

        # special case for tested api, it returns 'ReportDate' instead of 'Reportdt'
        if record_type == 'tested':
            record['Reportdt'] = record['ReportDate']

        records[record_type].append({
            "timestamp": record['Reportdt'],
            "date": datetime.fromtimestamp(record['Reportdt']/1000).strftime('%Y-%m-%d'),
            "indicator": "Daily",
            "city_en": record['PlaceName_EN'] if 'PlaceName_EN' in record else "total",
            "city_ar": record['PlaceName_AR'] if 'PlaceName_AR' in record else "الكل",
            "region_en": record['RegionName_EN'] if 'RegionName_EN' in record else "total",
            "region_ar": record['RegionName_AR'] if 'RegionName_AR' in record else "الكل",
            "case_type": record_type,
            "case_value": record[api['record_type'][record_type]['field']]
        })

    # check if there is more data go grap it
    if api['allow_pagination'] and len(res['features']) == api['data_count']:
        page += 1
        return getRecords(record_type, page)


def calculateActiveCases():
    """
        calculate daily active cases from cumulative cases
        daily_active = cumulative_confirmed - cumulative_recovery - cumulative_death
    """
    city_day = {}
    for case_type_record in cumulative_records:
        if case_type_record == 'confirmed' or case_type_record == 'recovery' or case_type_record == 'death':
            # create a factor to multiply by
            if case_type_record == 'confirmed':
                factor = 1
            else:
                factor = -1

            for record in cumulative_records[case_type_record]:
                _key = '{}_{}'.format(record['city_en'], record['date'])
                _temp = copy.deepcopy(record)
                if _key not in city_day:
                    _temp['case_value'] *= factor
                    _temp['case_type'] = "active"
                    city_day[_key] = _temp
                else:
                    city_day[_key]['case_value'] += _temp['case_value'] * factor

    # store daily records
    for record in city_day.values():
        records['active'].append(record)


def accumulate(types):
    """
        Daily accumulate records for each case_type & city (if applicable)
    """
    for case_type_record in records:
        if case_type_record not in types:
            continue

        accumulated_city = {}

        for record in records[case_type_record]:
            if record['city_en'] not in accumulated_city:
                accumulated_city[record['city_en']] = record['case_value']
            else:
                accumulated_city[record['city_en']] += record['case_value']
                temp_record = copy.deepcopy(record)
                temp_record['indicator'] = "Cumulative"
                temp_record['case_value'] = accumulated_city[record['city_en']]
                cumulative_records[case_type_record].append(temp_record)


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
    workbook = xlsxwriter.Workbook(file_name)
    worksheet = workbook.add_worksheet()

    # adding columns headers
    columns = ["timestamp",
               "date",
               "indicator",
               "city_en",
               "city_ar",
               "region_en",
               "region_ar",
               "case_type",
               "case_value"]

    # write column_name in the first row
    for column_name in columns:
        col = columns.index(column_name)  # order.
        worksheet.write(0, col, column_name)

    # write bulk records
    row = 1
    for case_type_record in records:
        for record in records[case_type_record]:
            for _key, _value in record.items():
                col = columns.index(_key)
                worksheet.write(row, col, _value)
            row += 1  # go next row

        for record in cumulative_records[case_type_record]:
            for _key, _value in record.items():
                col = columns.index(_key)
                worksheet.write(row, col, _value)
            row += 1  # go next row

    workbook.close()


if __name__ == '__main__':
    getRecords('confirmed')
    getRecords('recovery')
    getRecords('death')
    getRecords('critical')
    getRecords('tested')
    accumulate(["confirmed", "recovery", "death", "critical", "tested"])
    calculateActiveCases()
    accumulate(['active'])
    writeBulkToExcel()
