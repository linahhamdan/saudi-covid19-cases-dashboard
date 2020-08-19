"""
    This script service as a data extracter for all covid-19 records in Saudi Arabia 
    form the same source used by MOH covied-19 dashboard
"""
#imports
import json
import requests
from datetime import datetime
import xlsxwriter

# records list
records = []

# file name to store records
file_name = 'records_test.xlsx'

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
    print('Getting records for {} cases [page {}]...'.format(
        record_type, page))
    # construct full url
    results_count = '&resultOffset={}&resultRecordCount={}'.format(
        api['data_count']*page, api['data_count'])
    full_url = '{}{}{}{}'.format(
        api['base_url'],
        api['record_type'][record_type]['url'],
        api['date_order'],
        results_count)

    # send request
    res = _requester(full_url)
    print('Records in this request: {}'.format(len(res['features'])))

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
    else:
        # write all records to excel
        print('Collected {} {} records ...\n'.format(len(records), record_type))
        return


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
    print('writing records to {} ...'.format(file_name))
    # create and setup work sheet
    workbook = xlsxwriter.Workbook(file_name)
    worksheet = workbook.add_worksheet()
    
    # adding columns headers
    columns = ["timestamap",
               "date",
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
    for record in records:
        for _key, _value in record.items():
            col = columns.index(_key)
            worksheet.write(row, col, _value)
        row += 1  # go next row

    print('Done writing records!')
    workbook.close()


if __name__ == '__main__':
    getRecords('confirmed', 0)
    getRecords('recovery', 0)
    getRecords('death', 0)
    getRecords('critical', 0)
    getRecords('tested', 0)
    writeBulkToExcel()
