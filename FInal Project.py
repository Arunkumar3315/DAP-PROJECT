#!/usr/bin/env python
# coding: utf-8

# In[20]:


import pandas as pd
import csv
import numpy as np
from scipy import stats
from sklearn import preprocessing
from sklearn.model_selection import KFold
from sklearn.linear_model import LinearRegression
import plotly.express as px
import numpy as np
import pandas as  pd
                           
                             

final_df = pd.read_csv("python_files/final_csv.csv")  
#############################SPLITTING THE DATAFRAME FOR REGRESSION ANALYSIS#################


pop=pd.DataFrame(final_df['population'])
unemp=pd.DataFrame(final_df['unemployment_rate'])
pci=pd.DataFrame(final_df['per_capita_income'])


############################REGRESSION ANALYSIS ON POPULATION AND UNEMPLOYEMENT#######################


pop=pd.DataFrame(final_df['population'])
unemp=pd.DataFrame(final_df['unemployment_rate'])


model = LinearRegression()
scores = []
kfold = KFold(n_splits=3, shuffle=True, random_state=42)
for i, (train, test) in enumerate(kfold.split(pop,unemp)):
 model.fit(pop.iloc[train,:], unemp.iloc[train,:])
 score = model.score(pop.iloc[test,:], unemp.iloc[test,:])
 scores.append(score)
print("Regression score for population and unemployement is : ", scores)


############################REGRESSION ANALYSIS ON POPULATION AND PER CAPITA INCOME#######################

final_df = pd.read_csv("python_files/final_csv.csv")
pop=pd.DataFrame(final_df['population'])
unemp=pd.DataFrame(final_df['unemployment_rate'])
pci=pd.DataFrame(final_df['per_capita_income'])
model = LinearRegression()
scores = []
kfold = KFold(n_splits=3, shuffle=True, random_state=42)
for i, (train, test) in enumerate(kfold.split(pop,pci)):
 model.fit(pop.iloc[train,:], pci.iloc[train,:])
 score = model.score(pop.iloc[test,:], pci.iloc[test,:])
 scores.append(score)
print("Regression score for population and per capita income is : ", scores)

############################REGRESSION ANALYSIS ON PER CAPITA INCOME AND UNEMPLOYMENT RATE#######################


final_df = pd.read_csv("python_files/final_csv.csv")
unemp=pd.DataFrame(final_df['unemployment_rate'])
pci=pd.DataFrame(final_df['per_capita_income'])
model = LinearRegression()
scores = []
kfold = KFold(n_splits=3, shuffle=True, random_state=42)
for i, (train, test) in enumerate(kfold.split(unemp,pci)):
 model.fit(unemp.iloc[train,:], pci.iloc[train,:])
 score = model.score(unemp.iloc[test,:], pci.iloc[test,:])
 scores.append(score)
print("Regression score for per capita income and unemployement rate is : ", scores)



######################################DATA VISUALISATION #####################################################
###################################SCATTER PLOT#########################################################

scat = px.scatter(final_df, x="population", y="per_capita_income")
scat.show()

scat = px.scatter(final_df, x="unemployment_rate", y="per_capita_income")
scat.show()

########################################BAR GRAPH#####################################################

bar = px.bar(final_df, x='region_name', y='per_capita_income')
bar.show()

bar = px.bar(final_df, x='region_name', y='unemployment_rate')
bar.show()

bar = px.bar(final_df, x='region_name', y='population')
bar.show()


# In[ ]:




