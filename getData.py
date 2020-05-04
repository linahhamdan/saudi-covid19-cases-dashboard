# https://developer.github.com/v3/repos/contents/#create-or-update-a-file


import requests
from datetime import datetime
import time
import os
import csv
import pandas as pd
from urllib.parse import urljoin
import cloudinary
from cloudinary.models import CloudinaryField
from base64 import b64encode


replace={
    # "04/26/2020":{ "Yanbu": { "Daily_Confirmed":  0, "Daily_Active":  0, "Daily_Recovered":  0, "Daily_Deaths":  0},
    #               "Tabuk": { "Daily_Confirmed":  97675, "Daily_Active":  0, "Daily_Recovered":  0, "Daily_Deaths":  0}
    #              },

}


if __name__ == '__main__':
    daily_cases_url = 'https://services8.arcgis.com/uiAtN7dLXbrrdVL5/ArcGIS/rest/services/Saudi_COVID19_Cases/FeatureServer/1/query?where=1%3D1&objectIds=&time=&resultType=none&outFields=*&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnDistinctValues=false&cacheHint=false&orderByFields=Reportdt&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&sqlFormat=none&f=pjson&token=';
    response = requests.get(daily_cases_url)
    data=[]
    events_url='https://raw.githubusercontent.com/linahhamdan/saudi-covid19-cases-dashboard/master/events.json'
    regions_url='https://raw.githubusercontent.com/linahhamdan/saudi-covid19-cases-dashboard/master/regions.json'

    response_events = requests.get(events_url)
    response_regions = requests.get(regions_url)
    
    events=response_events.json()
    regions=response_regions.json()
    if response.status_code == 200 and response_events.status_code == 200 and response_regions.status_code == 200:
        print('Success!')
        cities_perday={}
        #get firt date
        previous_date= time.strftime('%m/%d/%Y',time.localtime(response.json()["features"][0]["attributes"]["Reportdt"]/1000))
        # print(previous_date)
        for row in response.json()["features"]:
            date=time.strftime('%m/%d/%Y',time.localtime(row["attributes"]["Reportdt"]/1000))
            city=row["attributes"]["Name_Eng"]
            confirmed = row["attributes"]["Confirmed"]
            active = row["attributes"]["Active"]
            recovered = row["attributes"]["Recovered"]
            deaths = row["attributes"]["Deaths"]

            

            if (date in replace) and (city in replace[date]):
                confirmed = replace[date][city]["Daily_Confirmed"]
                active = replace[date][city]["Daily_Active"]
                recovered = replace[date][city]["Daily_Recovered"]
                deaths = replace[date][city]["Daily_Deaths"]
            data.append({"Date":date, "City":city,
                        "Daily_Confirmed":  confirmed, 
                        "Daily_Active": active,
                        "Daily_Recovered":  recovered,
                        "Daily_Deaths":  deaths})
            
            if date not in cities_perday.keys():
                cities_perday[date]=[city]
            else:
                cities_perday[date].append(city)
        
        for d in cities_perday:
            for c in cities_perday[previous_date]:
                if  c not in cities_perday[d]:
                    cities_perday[d].append(c)
                    data.append({"Date":d, "City":c,
                                "Daily_Confirmed":  0, 
                                "Daily_Active":  0,
                                "Daily_Recovered":  0,
                                "Daily_Deaths":  0})
            previous_date=d

        df = pd.DataFrame(data)
        dfg= df.groupby(['Date', 'City']).agg({'Daily_Confirmed': "sum", 'Daily_Active': "sum",
        'Daily_Recovered': 'sum',  'Daily_Deaths': "sum"}).reset_index()
        

        dfg['Cumulative_Confirmed'] =  dfg.groupby(by=['City'])['Daily_Confirmed'].transform(lambda x: x.cumsum())
        dfg['Cumulative_Active'] = dfg.groupby(by=['City'])['Daily_Active'].transform(lambda x: x.cumsum())
        dfg['Cumulative_Recovered'] = dfg.groupby(by=['City'])['Daily_Recovered'].transform(lambda x: x.cumsum())
        dfg['Cumulative_Deaths'] = dfg.groupby(by=['City'])['Daily_Deaths'].transform(lambda x: x.cumsum())
        
        dfg['Region'] = dfg['City'].map(regions)
        dfg['Event'] = dfg['Date'].map(events)
        dfg.to_csv(r'COVID19-Cases-SaudiArabia.csv',index=False, header=True)
        df.to_json(r'COVID19-Cases-SaudiArabia.json')





        # first: reading the binary stuff
        # note the 'rb' flag
        # result: bytes
        with open('COVID19-Cases-SaudiArabia.csv', 'rb') as open_file:
            byte_content = open_file.read()
        # second: base64 encode read data
        # result: bytes (again)
        base64_bytes = b64encode(byte_content)
        # third: decode these bytes to text
        base64_string = base64_bytes.decode('utf-8')


        url_github='https://api.github.com/repos/linahhamdan/saudi-covid19-cases-dashboard/contents/COVID19-Cases-SaudiArabia.csv'
        
        headers = {
            'Authorization': 'token *******',
        }
        response_get = requests.get(url_github, headers=headers)
        sha = response_get.json()["sha"]
        data = '{"message": "uploading new data","sha":"'+sha+'", "content":"'+base64_string+'"}'

        response_post = requests.put(url_github, headers=headers, data=data)
        print(response_post.json())



        

        # cloudinary.config( 
        #     cloud_name = "dofu9jayl", 
        #     api_key = "785387994283178", 
        #     api_secret = "4kDoM4GqPf6-tEBp2w-or-PKQ_8" 
        # )
        # cloudinary.uploader.upload("COVID19-Cases-SaudiArabia.csv",folder='covid19-cases-saudi-dataset',
        # use_filename='true',unique_filename='false', resource_type = "auto", overwrite='true')


    elif response.status_code == 404:
        print('Not Found.')
        