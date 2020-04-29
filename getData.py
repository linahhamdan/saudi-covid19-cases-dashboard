import requests
from datetime import datetime
import time
import os
import csv
import pandas as pd
# import cloudinary
# from cloudinary.models import CloudinaryField


# cloudinary.config( 
#   cloud_name = "dofu9jayl", 
#   api_key = "785387994283178", 
#   api_secret = "4kDoM4GqPf6-tEBp2w-or-PKQ_8" 
# )

replace={
    # "04/26/2020":{ "Yanbu": { "Daily_Confirmed":  0, "Daily_Active":  0, "Daily_Recovered":  0, "Daily_Deaths":  0},
    #               "Tabuk": { "Daily_Confirmed":  97675, "Daily_Active":  0, "Daily_Recovered":  0, "Daily_Deaths":  0}
    #              },

}
events={
        "03/02/2020": "First case of COVID-19",
        "03/04/2020": "Umrah suspension",
        "03/09/2020": "flights suspended to number of countries",
        "03/15/2020": "International flights suspension for 14 days",
        "03/16/2020": "Gov / private suspension ",
        "03/21/2020": "Domestic flights suspension",
        "03/23/2020": "Curfew started for 21 days (6am -7 pm)",
        "03/26/2020": "Riyadh, Makkah and Madinah lockdown - curfew (6am - 3pm)",
        "03/29/2020": "Jeddah lockdown",
        "03/30/2020": "Makkah , Madinah 24 hours curfew",
        "04/02/2020": "Makkah lockdown",
        "04/04/2020": "Jeddah areas lockdown - 24 h curfew",
        "04/06/2020": "Riyadh, Dammam , Tabuk , Dahran, Hafuf, Jeddah, Taif, Qatif , Khobar24h curfew",
        "04/25/2020": "Partial lifting of curfew in all cities except Makkah"
       }


regions={

'Al Majarda' : 'Asir',  
'Abu Arish' : 'Jazan', 
'Al Bahah' : 'Al Bahah', 
'Al Badayea' : 'Qassim', 
'Al Hanakia' : 'Qassim', 
'Al Mubarraz' : 'Eastern Region', 
'Ahad Rafidah' : 'Asir', 
'Al Wajh' : 'Tabuk', 
'Al Qunfudhah' : 'Makkah', 
'Arar' : 'Northern Borders', 
'Al Duwadimi' : 'Riyadh', 
'Ar Rass' : 'Qassim', 
'Al Majmaah' : 'Riyadh', 
'Al Ula' : 'Medina', 
'Aridah' : 'Jazan', 
'Al Shamly' : 'Hail', 
'Alqarei' : 'Makkah', 
'Al Makhwa' : 'Al Bahah', 
'Almuzaylif' : 'Makkah', 
'Az Zulfi' : 'Riyadh', 
'Al Jafr' : 'Eastern Region', 
'Al Muwayh' : 'Makkah', 
'Adham' : 'Makkah', 
'Al Lith' : 'Makkah', 
'Al Tuwal' : 'Jazan',
'Al Quwaiiyah' : 'Riyadh', 
'Al Bukayriyah' : 'Qassim',
'Al Aqiq' : 'Al Bahah', 
'Al Mandaq' : 'Al Bahah', 
'Al Khurma' : 'Makkah', 
'Abha' : 'Asir', 
'Al Ais' : 'Medina', 
'Al Madda' : 'Asir', 
'Al Mithnab' : 'Qassim', 
'Al Hada' : 'Makkah', 

'Baish' : 'Jazan', 
'Baljurashi' : 'Al Bahah', 
'Bqeeq' : 'Eastern Region', 
'Bani Malek' : 'Makkah', 
'Bisha' : 'Asir', 
'Buraydah' : 'Qassim', 
'Duba' : 'Tabuk', 
'Diriyah' : 'Riyadh',
'Dammam' : 'Eastern Region', 
'Dhahran' : 'Eastern Region', 


'Hofuf' : 'Eastern Region', 
'Hafar Al Batin' : 'Eastern Region', 
"Ha'il" : 'Hail', 
'Hadda' : 'Makkah', 
'Howtat Bani Tamim' : 'Riyadh', 
'Howtat Sudair' : 'Riyadh', 

'Mahd Al Dhahab' : 'Medina', 
'Muzahmiya' : 'Riyadh', 
'Muhayil Aseer' : 'Asir', 
'Medina' : 'Medina', 
'Maisan' : 'Makkah', 
'Mecca' : 'Makkah', 
'Nairyah' : 'Eastern Region', 
'Najran' : 'Najran', 


'Qatif' : 'Eastern Region', 
'Riyadh' : 'Riyadh', 
'Ras Tanura' : 'Eastern Region', 

'Jeddah' : 'Makkah', 
'Jazan' : 'Jazan', 
'Jubail' : 'Eastern Region', 

'Tabuk' : 'Tabuk', 
"Ta'if" : 'Makkah', 
'Khafji' : 'Eastern Region', 
'Khamis Mushait' : 'Asir', 
'Kharj' : 'Riyadh', 
'Khulais' : 'Makkah', 
'Khobar' : 'Eastern Region', 
 


'Qurayyat' : 'Tabuk', 

'Riyadh Al Khabra' : 'Qassim', 
'Rafha' : 'Northern Borders', 
'Rabigh' : 'Makkah', 

'Sajer' : 'Riyadh', 
'Sabt Al Alayah' : 'Asir', 
'Sabya' : 'Jazan',
'Samtah' : 'Jazan', 
'Saihat' : 'Eastern Region', 
'Sakakah' : 'Al Jouf', 
'Sharorah' : 'Najran', 

'Tabarjal' : 'Al Jouf', 
'Turbah' : 'Makkah',  
'Thurayban' : 'Asir', 
'Uqlat as Suqur' : 'Qassim', 
'Umm Aldoom' : 'Makkah', 
'Unayzah' : 'Qassim', 

"Wadi Al Fara'a" : 'Medina',
'Wadi Al Fara’a' : 'Medina', 
'Wadi Addawasir' : 'Riyadh',
'Yanbu' : 'Medina', 


}




if __name__ == '__main__':
    daily_cases_url = 'https://services8.arcgis.com/uiAtN7dLXbrrdVL5/ArcGIS/rest/services/Saudi_COVID19_Cases/FeatureServer/1/query?where=1%3D1&objectIds=&time=&resultType=none&outFields=*&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnDistinctValues=false&cacheHint=false&orderByFields=Reportdt&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&sqlFormat=none&f=pjson&token=';
    response = requests.get(daily_cases_url)
    data=[]
    
    if response.status_code == 200:
        print('Success!')
        cities_perday={}
        #get firt date
        previous_date= time.strftime('%m/%d/%Y',time.localtime(response.json()["features"][0]["attributes"]["Reportdt"]/1000))
        print(previous_date)
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
        # dfg.to_csv(r'COVID19-Cases-SaudiArabia.csv',index=False, header=True)
        # cloudinary.uploader.upload("COVID19-Cases-SaudiArabia.csv",resource_type = "auto")
    elif response.status_code == 404:
        print('Not Found.')

