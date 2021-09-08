# -*- coding: utf-8 -*-
"""
Created on Mon Sep  6 17:13:44 2021

@author: Shrivatsan Ragavan
"""

import requests
import psycopg2
import pandas as pd


def ACS5_API(get_vars,year,col_names,state_num):
    #get course data from main list
    host = 'https://api.census.gov/data'
    #year='2019'
    data='acs/acs5'
    base_url='/'.join([host,year,data])
    predicates={}
    #get_vars= ["NAME","B01003_001E","B25051_001E","B25041_001E","B01001_002E","B01001_026E"]
    #col_names=["NAME","Total Population","Total Kitchen Facilities","Total Bedrooms","Total Males","Total Females","State","County","Tract","Block Group"]
    predicates["get"]=",".join(get_vars)
    predicates["for"]='block group:*'
    predicates["in"]='state:'+state_num+'%20county:*'
    r=requests.get(base_url,params=predicates)
    return r

def get_df(r,col_names):
    df=pd.DataFrame(columns=col_names,data=r.json()[1:])
    df["Total Population"]=df["Total Population"].astype(int)
    df["Total Kitchen Facilities"]=df["Total Kitchen Facilities"].astype(int)
    df["Total Bedrooms"]=df["Total Bedrooms"].astype(int)
    df["Total Males"]=df["Total Males"].astype(int)
    df["Total Females"]=df["Total Females"].astype(int)
    return df

def pgsql(df):
    conn=psycopg2.connect("host=acs-db.mlpolicylab.dssg.io dbname=acs_data_loading user=mlpp_student password=CARE-horse-most port=5432")
    cur=conn.cursor()
    cur.execute("""
        CREATE TABLE ACS.SRAGAVAN_ACS_DATA(
            NAME TEXT PRIMARY KEY,
            TOTAL_POPULATION INTEGER,
            TOTAL_KITCHENS INTEGER,
            TOTAL_BEDROOMS INTEGER,
            TOTAL_MALES INTEGER,
            TOTAL_FEMALES INTEGER,
            STATE TEXT,
            COUNTY TEXT,
            TRACT INTEGER,
            BLOCK_GROUP INTEGER
            )
            """)
    conn.commit()
    for x, y in df.iterrows():    
        cur.execute(
            "INSERT INTO ACS.SRAGAVAN_ACS_DATA VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s )",
            (y[0],y[1],y[2],y[3],y[4],y[5],y[6],y[7],y[8],y[9])
                    )
    conn.commit()
    

def main():
    year1='2019'
    get_vars1= ["NAME","B01003_001E","B25051_001E","B25041_001E","B01001_002E","B01001_026E"]
    col_names1=["NAME","Total Population","Total Kitchen Facilities","Total Bedrooms","Total Males","Total Females","State","County","Tract","Block Group"]
    state_num1='42'
    
    #Call the API
    r1=ACS5_API(get_vars1,year1,col_names1,state_num1)
    fin_df=get_df(r1,col_names1)
    pgsql(fin_df)

if __name__=='__main__':
    main()
    