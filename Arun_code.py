#!/usr/bin/env python
# coding: utf-8

# In[28]:


import pymongo
from pymongo import MongoClient
import json
import pandas as pd
import pprint
from pandas.io.json import json_normalize
import numpy as np

##########establishing connection with mongodb##############################


client=MongoClient('mongodb://localhost:27017')
db=client['unemployment_database']
collection=db['unemployment_data']

###############################inserting data to mongodb#######################




try:
    with open("python_files/unemployment_data.json") as f:
        file_data = json.load(f)
        collection.insert_many(file_data)
except:
    print("error while inserting")
finally:
    print("insertion operation has been performed")
    client.close()

    

##########reading data into an array and converting to csv####################


try:
    arr=list(file_data)
    keys = arr[0].keys()
    #print(keys)
    csv=pd.DataFrame(arr).to_csv("python_files/output.csv")
except:
    print("error while converting")
finally:
    print("operation of converting to csv file is done")
    
    

###converting the structured csv file to a dataframe for cleaning and preprocessing#######

converted_csv=pd.read_csv('python_files/output.csv')
to_drop = ['datasetid','recordid','record_timestamp','_id']
dropped=converted_csv.drop(to_drop, inplace=True, axis=1)
converted_csv.head()
converted_csv[['unemployment_rate','metropolitian_area']]= converted_csv.fields.str.split(",",n=1,expand=True,)
converted_csv.head()
converted_csv=converted_csv.drop(columns='fields')
converted_csv.head()


converted_csv[['','unemployment_rate']]= converted_csv.unemployment_rate.str.split("{",n=1,expand=True)
converted_csv = converted_csv[converted_csv.unemployment_rate != "'year': '2015'}"]
converted_csv = converted_csv[converted_csv.unemployment_rate != "'year': '2014'}"]
converted_csv = converted_csv[converted_csv.unemployment_rate != "'year': '2017'}"]
converted_csv= converted_csv.rename(columns={"Unnamed: 0": "sl_no","unemployment_rate":"unemployment_rate"})
converted_csv.head()



converted_csv[['metropolitian_area','extra']]= converted_csv.metropolitian_area.str.split(",",n=1,expand=True,)
converted_csv[['extra','toreduce']]= converted_csv.extra.str.split(",",n=1,expand=True,)
converted_csv=converted_csv.drop(columns='extra')
converted_csv[['namelsad','rank']]= converted_csv.toreduce.str.split(",",n=1,expand=True,)
converted_csv=converted_csv.drop(columns='namelsad')
converted_csv[['namelsad','rank']]= converted_csv.toreduce.str.split(",",n=1,expand=True,)
converted_csv=converted_csv.drop(columns='namelsad')
converted_csv=converted_csv.drop(columns='rank')
converted_csv[['namelsad','ranks']]= converted_csv.toreduce.str.split(",",n=1,expand=True,)
converted_csv[['namelsad','ranks']]= converted_csv.ranks.str.split(",",n=1,expand=True,)
converted_csv=converted_csv.drop(columns='namelsad')
converted_csv=converted_csv.drop(columns='toreduce')
converted_csv[['ranks','year']]= converted_csv.ranks.str.split(",",n=1,expand=True,)
converted_csv=converted_csv.drop(columns='year')
converted_csv[['extra','rank']]= converted_csv.ranks.str.split(":",n=1,expand=True,)
converted_csv=converted_csv.drop(columns='extra')
converted_csv=converted_csv.drop(columns='ranks')
converted_csv[['extra','metropolitian_area']]= converted_csv.metropolitian_area.str.split(":",n=1,expand=True,)
converted_csv=converted_csv.drop(columns='extra')
converted_csv[['extra','metropolitian_area']]= converted_csv.metropolitian_area.str.split("'",n=1,expand=True,)
converted_csv=converted_csv.drop(columns='extra')
converted_csv[['extra','unemployment_rate']]= converted_csv.unemployment_rate.str.split(":",n=1,expand=True,)
converted_csv=converted_csv.drop(columns='extra')
converted_csv.head()

####################CLEANED DATAFRAME TO CSV CONVERSION######################


converted_csv.to_csv(r'python_files/cleaned_file.csv',index=False)


###############POSTGRESQL CONNECTION#########################################


import psycopg2

dbConnection=psycopg2.connect(
                host="localhost",
                database="postgres",
                user="postgres",
                password="dap")
dbCursor=dbConnection.cursor()

###########CREATING A TABLE IN POSTGRES###################################


try:
    dbCursor.execute("""CREATE TABLE unemployment_table
    (sl_no text PRIMARY KEY,
    unemployment_rate text,
    metropolitian_area text,
    rank text
    )""")
    print("Table created ")
except:
    print("error in creating table")
finally:
    if(dbConnection):
        dbCursor.close()
        dbConnection.close()

#############INSERTNG VALUES INTO TABLE##################################


try:
    dbCursor=dbConnection.cursor()
    with open('python_files/cleaned_file.csv', 'r') as f:
        reader = csv.reader(f)
        next(reader) # Skip the header row.
        for row in reader:
            print(row)
            dbCursor.execute("INSERT INTO unemployment_table(sl_no,unemployment_rate,metropolitian_area,rank) VALUES (%s,%s,%s,%s)",(row))
            print("values inserted")
except:
    print("error in inserting values")
finally:
    if(dbConnection):
        dbCursor.close()
        dbConnection.close()

#################reading values from postgresql#############################



import psycopg2

try:

    dbConnection=psycopg2.connect(
                host="localhost",
                database="postgres",
                user="postgres",
                password="dap")
    dbCursor=dbConnection.cursor()
    postgreSQL_select_Query = "SELECT * FROM unemployment_table"
    dbCursor.execute(postgreSQL_select_Query)
    for table in dbCursor.fetchall():
        print(table)
except:    
    print("error in reading values")
    
finally:
    if(dbConnection):
        dbCursor.close()
        dbConnection.close()


    
###########################PLOTTING #############################################

import psycopg2 as pg
import pandas as pd
import pandas.io.sql as psql
import numpy as np
import plotly.express as px
import matplotlib.pyplot as plt
import seaborn as sns


try:
    dbConnection=psycopg2.connect(
                host="localhost",
                database="postgres",
                user="postgres",
                password="dap")
    dbCursor=dbConnection.cursor()
    table    = pd.read_sql('SELECT * FROM unemployment_table', dbConnection)
    dataframe=pd.DataFrame(table)
    fig = px.bar(dataframe, x='metropolitian_area', y='unemployment_rate')
    fig.show()
    histogram=px.histogram(dataframe, x="unemployment_rate")
    histogram.show()

    bar_graph=px.bar(dataframe, x='metropolitian_area', y='unemployment_rate')
    bar_graph.show()
    scatter = px.scatter(dataframe, x="metropolitian_area", y="unemployment_rate")
    scatter.show()
except:
    print("error in visulaizing")
finally:
    if(dbConnection):
        dbCursor.close()
        dbConnection.close()











    

    

