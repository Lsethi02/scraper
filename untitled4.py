#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May 29 08:48:58 2019

@author: lakshittasethi
"""

import pandas as pd
from bs4 import BeautifulSoup
from requests import get
from requests.exceptions import RequestException

from contextlib import closing


def get_url(url,outPath):
   
    '''
    Gets url and designated file output and returns the html file from which 
    data can be extracted using the soup module
    '''
    try:
        with closing(get(url, stream=True)) as resp:
            if is_good_response(resp):
                response = resp.content
                html = response.decode()
                with open(outPath , 'w') as fout:
                    fout.write(html)
                fout.close()
                raw_html = open(outPath).read()
                wiki_html = BeautifulSoup(raw_html, 'html.parser')
                return wiki_html
                
            else:
                return None

    except RequestException as e:
        log_error('Error during requests to {0} : {1}'.format(url, str(e)))
        return None


def is_good_response(resp):
    """
    Returns True if the response seems to be HTML, False otherwise.
    """
    content_type = resp.headers['Content-Type'].lower()
    return (resp.status_code == 200 
            and content_type is not None 
            and content_type.find('html') > -1)


def log_error(e):
   
    print(e)
    
    
    
    #Scraping data about cities by population from wikipedia 
    
wiki=get_url("https://en.wikipedia.org/wiki/List_of_United_States_cities_by_population","wiki_info.html")
table = wiki.find("table", {"class":"wikitable sortable"})

rank=[]
city=[]
state=[]
estimate18=[]
census10=[]
change=[]
land2016=[]
density=[]
location=[]

for row in table.find_all('tr')[1:]:
    col=row.find_all('td')
    col1=col[0].string.strip()
    rank.append(col1)
    col2=col[1].text.strip()
    city.append(col2)
    col3=col[2].text.strip()
    state.append(col3)
    col4=col[3].string.strip()
    estimate18.append(col4)
    col5=col[4].string.strip()
    census10.append(col5)
    col6=col[5].text.strip()
    change.append(col6)
    col7=col[6].string.strip()
    land2016.append(col7)
    col8=col[8].text.strip()
    density.append(col8)
    col9=col[10].text.strip()
    location.append(col9)

columns={'rank':rank,'city':city,'state':state,'2018 Estimate':estimate18,'2010 Census':census10,'change':change,
         '2016 land area':land2016,'2016 population density':density,'location':location}
df=pd.DataFrame(columns)
#print(df)

df.to_csv("population_table",sep='\t',header=True)

# Scraping data of unemployment rate by cities from Wikipedia

unemp_rate=get_url("https://www.bls.gov/lau/lacilg16.htm","unemp_rate_info.html")
table_unemp = unemp_rate.find("table",attrs={"class":"regular"})
unemp_table=table_unemp.find('tbody')

e_rate=[]
rank1=[]
city1=[]

for row1 in unemp_table.find_all('tr')[1:]:
    cmn=row1.find_all('td')
    cmn1=cmn[0].string.strip()
    e_rate.append(cmn1)
    cmn2=cmn[1].string.strip()
    rank1.append(cmn2)
    cell=row1.find_all('th')
    cell1=cell[0].string.strip()
    city1.append(cell1)

cmns={'City':city1,'Unemployment rate 2016':e_rate,'rank':rank1}
df1=pd.DataFrame(cmns)
df1.to_csv("unemloyment_table",sep='\t')

# Data about cities by per capita income

income=get_url("https://en.wikipedia.org/wiki/List_of_United_States_metropolitan_areas_by_per_capita_income","income.html")
income_table=income.find("table", {"class":"toccolours sortable"})

area=[]
cap=[]

for row2 in income_table.find_all('tr')[1:]:
    cl=row2.find_all('td')
    cl1=cl[1].text.strip()
    area.append(cl1)
    cl2=cl[3].string.strip()
    cap.append(cl2)
    
cls= {'City':area,'per Capita Income':cap}
df2=pd.DataFrame(cls)
df2.to_csv("table_Income",sep='\t')





















