# -*- coding: utf-8 -*-


#%%

from datetime import datetime
startTime =datetime.now()
import pandas as pd
"""
import numpy as np
import glob
import matplotlib.pyplot as plt
import sys
import time
import datetime
from datetime import timedelta
import matplotlib.gridspec as gridspec
import matplotlib.cm as mpl_cm
from scipy.stats import ttest_ind
from scipy.stats import ttest_rel
from scipy import stats
"""
import string
pd.options.display.float_format = '{:.6f}'.format
#%%   READ IN DATA FILES
# Read in user data 
user = pd.read_csv("user.csv", 
                      low_memory=False)

# Read in activity data 
mylist = []
for chunk in pd.read_csv("activitydf.csv",chunksize=1000,low_memory=False):
    mylist.append(chunk)
active = pd.concat(mylist, axis= 0)
del mylist
#%% CLEAN USER DATA: Age and gender

# calculate age of users 
# data collected in 2016
user["Age_calc"] = "2016"

# calculate age in 2016
user["Age"] =  pd.to_numeric(user["Age_calc"])- pd.to_numeric(user["Year_Of_Birth"])
# remove column used to calculate age
user = user.drop("Age_calc", axis=1)
# ensure consistent age formatting
user['Age']= user['Age'].astype(float)

# consistent Gender label formatting 
user["Gender"]= user["Gender"].replace({"Female":"female"})
user["Gender"]= user["Gender"].replace({"Male":"male"})
user["Gender"] =user["Gender"].fillna("Unknown")

user.head(),user.shape # n=539979


### CLEANING PseudoUserId ####
user= user.drop(user.loc[user['PseudoUserId'].duplicated()==True].index)
user.shape # n=539957


### CLEANING AGE ####


# create dataframes for known and unknown ages
# can use for later statistical comparison - is there a difference in gender between those who choose to enter their age
dem_users_unknown_age = pd.concat([user.loc[(user['Age']>100)],user.loc[(user['Age']<18)], user.loc[(user['Age'].isnull())]])
dem_users_known_age = user.loc[(user['Age']<=100) & (user['Age']>=18)]
# check add up to 539957 (145962 + 393995)
dem_users_unknown_age.shape[0]+ dem_users_known_age.shape[0]

# Number of users with unknown or excluded age: 145,962
dem_users_unknown_age.shape #145962
# Of these users (uncomment to check)
    # Users with no recorded age:
    # user.loc[user['Age'].isnull()].shape # 135,381
    # Users older than 100:
    # user.loc[(user['Age']>100)].shape #493
    # Users younger than 18:
    # user.loc[(user['Age']<18)].shape # 10,088

    # Check add up to total number of users with unknown or excluded ages 
    # 135381 + 493 + 10088 # = 145962

# Number of users with known and included ages: 
dem_users_known_age.shape #393995

 # check no variables not in either dataframe (should return blank df)
temp =pd.concat([dem_users_known_age,dem_users_unknown_age])
user.loc[~user['PseudoUserId'].isin(temp['PseudoUserId'])].copy()


### CLEANING GENDER ####

# create dataframes for known and unknown genders
# located unknown gender
dem_users_unknown_gender =  user.loc[user["Gender"]=='Unknown']
# located known gender
dem_users_known_gender = pd.concat([user.loc[user["Gender"]=='male'], user.loc[user["Gender"]=='female']])
# check add up to 539957 (102784+ 437173)
dem_users_unknown_gender.shape[0]+ dem_users_known_gender.shape[0]


### drop unwanted variables
# drop the unknown + unwanted age variables 
# (only do once investigated other cleaning)
user = user.drop(pd.concat([user.loc[(user['Age']>100)],user.loc[(user['Age']<18)], user.loc[(user['Age'].isnull())]]).index)

# user.loc[user["Gender"]=='Unknown'].shape # 3138
# drop the unknown gender variables 
# (only do once investigated other cleaning)
user=user.drop(user.loc[user["Gender"]=='Unknown'].index)



#%% 
### POSTCODE CLEANING ###
# get postcode sector data 
sectors = pd.read_csv("postcode_sectors.csv", 
                      low_memory=False)
sectors_nssec = pd.read_csv("sector_nssec.csv", 
                      low_memory=False)
sectors = sectors.merge(sectors_nssec, left_on='Postdist',right_on='postdist', how='left')
sectors.to_csv("postcode_sectors_merged.csv")

# subset data
sectors['Sectlen_three'] =sectors['Postcode_nospace'].str.slice(stop=3)
sectors['SectFour'] =sectors['Postcode_nospace'].str.slice(stop=4)

print('Number of unique postcode districts:', sectors['Postdist'].nunique()) # 2960 unique postcode districts

#%%
# create temporary copy of users dataframe
df_user_temp = user
# remove users with no postcode 
df_user_temp = df_user_temp.drop(df_user_temp.loc[df_user_temp['Postcode'].isnull()].index)

# number of users with non-null postcode
df_user_temp.shape[0] # n= 359311


# remove letters and white space from the end of the postcode
df_user_temp['Postcode_stripletter'] = df_user_temp['Postcode'].str.rstrip(string.ascii_letters + string.whitespace)

def add_space(string, length):
    return ' '.join(string[i:i+length] for i in range(0,len(string),length))

# add column with postcode separated by a space after first 2 characters
df_user_temp['Postcode_space2'] = df_user_temp['Postcode'].apply(lambda x : add_space(x,2))
# remove letters and white space from the end of the postcode
df_user_temp['Postcode_space2'] = df_user_temp['Postcode_space2'].str.rstrip(string.ascii_letters + string.whitespace)

# Apply series of postcode length matching, there are several postal district formats
    # L: Letter
    # N: Number
    # e.g. LLNN: Letter Letter Number Number i.e. OX10 SO22, LS12

# LLNN postal district format
LLNN = df_user_temp.loc[(df_user_temp.Postcode.str[0].str.isalpha() ==True) & (df_user_temp.Postcode.str[1].str.isalpha() ==True) & (df_user_temp.Postcode.str[2].str.isnumeric() ==True)& (df_user_temp.Postcode.str[3].str.isnumeric() ==True)]
LLNN.shape[0] # n=310137

# LLNL postal district format
LLNL = df_user_temp.loc[(df_user_temp.Postcode.str[0].str.isalpha() ==True) & (df_user_temp.Postcode.str[1].str.isalpha() ==True) & (df_user_temp.Postcode.str[2].str.isnumeric() ==True)& (df_user_temp.Postcode.str[3].str.isalpha() ==True)]
LLNL.shape[0] # n=1868

# LNL postal district format
LNL = df_user_temp.loc[(df_user_temp.Postcode.str[0].str.isalpha() ==True) & (df_user_temp.Postcode.str[1].str.isnumeric() ==True) & (df_user_temp.Postcode.str[2].str.isalpha() ==True)]
LNL['LNL']= LNL['Postcode'].str.slice(stop=3)
LNL.shape[0] # n=2020

# LN postal district format
LN = df_user_temp.loc[(df_user_temp.Postcode.str[0].str.isalpha() ==True) & (df_user_temp.Postcode.str[1].str.isnumeric() ==True) & (df_user_temp.Postcode.str[2].str.isnumeric() ==True)& (df_user_temp.Postcode.str[3].str.isalpha() ==True)]
LN.shape[0] # n= 10772

# LNN postal district format
LNN = df_user_temp.loc[(df_user_temp.Postcode.str[0].str.isalpha() ==True) & (df_user_temp.Postcode.str[1].str.isnumeric() ==True) & (df_user_temp.Postcode.str[2].str.isnumeric() ==True)& (df_user_temp.Postcode.str[3].str.isnumeric() ==True)]
LNN['LNN'] = LNN['Postcode'].str.slice(stop=3)

# LNN_L postal district format
LNN_L = df_user_temp.loc[(df_user_temp.Postcode.str[0].str.isalpha() ==True) & (df_user_temp.Postcode.str[1].str.isnumeric() ==True) & (df_user_temp.Postcode.str[2].str.isnumeric() ==True)& (df_user_temp.Postcode.str[3].str.isalpha() ==True)]
LNN_L['Postcode_strip2']= LNN_L['Postcode_stripletter'].str.slice(stop=2)
LNN_L.shape[0] # n= 10772

# Get postcodes 3 characters long to avoid missing matches
len_three= df_user_temp.loc[df_user_temp['Postcode'].apply(lambda x: len(x) ==3) ]
len_three.shape[0] # n=603

# Get postcodes 2 characters long to avoid missing matches
len_two= df_user_temp.loc[df_user_temp['Postcode'].apply(lambda x: len(x) ==2) ]
len_two.shape[0] # n=252

LLNN = df_user_temp.loc[(df_user_temp.Postcode.str[0].str.isalpha() ==True) & (df_user_temp.Postcode.str[1].str.isalpha() ==True) & (df_user_temp.Postcode.str[2].str.isnumeric() ==True)& (df_user_temp.Postcode.str[3].str.isnumeric() ==True)]
LLNN.shape[0] # n= 310137



#%% Subset refernce sector data in same way 

# get sectors with the LLNL postal district format
sectors_LLNL = sectors.loc[(sectors.Postdist.str[0].str.isalpha() ==True) & (sectors.Postdist.str[1].str.isalpha() ==True) & (sectors.Postdist.str[2].str.isnumeric() ==True)& (sectors.Postdist.str[3].str.isalpha() ==True)].drop_duplicates(subset=['Postdist'])
sectors_LLNL.shape[0] # 51 unique postal districts with the LLNL format

# get sectors with the LNL postal district format
sectors_LNL = sectors.loc[(sectors.Postdist.str[0].str.isalpha() ==True) & (sectors.Postdist.str[1].str.isnumeric() ==True) & (sectors.Postdist.str[2].str.isalpha() ==True)].drop_duplicates(subset=['Postdist'])
sectors_LNL.shape[0] # 15 unique postal districts with the LLNL format

# get sectors with the LN postal district format
sectors_LN = sectors.loc[(sectors.Postcode_nospace.str[0].str.isalpha() ==True) & (sectors.Postcode_nospace.str[1].str.isnumeric() ==True) & (sectors.Postcode_nospace.str[2].str.isnumeric() ==True)].drop_duplicates(subset=['Postdist'])
sectors_LN.shape[0] # 336 unique postal districts with the LN format 

# get sectors with the LNN postal district format
sectors_LNN = sectors.loc[(sectors.Postdist.str[0].str.isalpha() ==True) & (sectors.Postdist.str[1].str.isnumeric() ==True) & (sectors.Postdist.str[2].str.isnumeric() ==True)].drop_duplicates(subset=['Postdist'])
sectors_LNN.shape[0] # 268 unique postal districts with the LN format

# get sectors with the LLN postal district format
sectors_LLN = sectors.loc[sectors['Postcode_nospace'].str.len()==4]
sectors_LLN.shape[0] # 5261 unique postal districts with the LLN format

# get sectors with the LLNN postal district format
sectors_LLNN = sectors.loc[sectors['Postcode_nospace'].str.len()==5]
sectors_LLNN.shape[0] # 5481 unique postal districts with the LLNN format


#%% join bounts postal districts to sector postla districts data to get cleaned postal districts. Across the different district formats. 
# get Bounts users with LLNL postal district format
LLNL_clean = LLNL.merge(sectors_LLNL, left_on='Postcode', right_on='Postdist', how='left').dropna(subset=['ns_sec_1'])
print(LLNL_clean.shape[0]) # n= 1597 users with LLNL postal district format

# get Bounts users with LNL postal district format
LNL_clean = LNL.merge(sectors_LNL, left_on='LNL', right_on='Postdist', how='left').dropna(subset=['ns_sec_1'])
print(LNL_clean.shape[0]) # n= 857 users with LNL postal district format

# get Bounts users with LN postal district format
LN_clean = LN.merge(sectors_LN, left_on='Postcode_space2', right_on='Postcode').dropna(subset=['ns_sec_1'])
print(LN_clean.shape[0]) # n= 2038 users with LNL postal district format

LNN_L_clean = LNN_L.merge(sectors.drop_duplicates(subset=['Postdist']), left_on='Postcode_stripletter',right_on='Postdist', how='left').dropna(subset=['ns_sec_1'])
print(LNN_L_clean.shape[0])

LNN_L_clean2 = LNN_L.merge(sectors.drop_duplicates(subset=['Postdist']), left_on='Postcode_strip2',right_on='Postdist', how='left').dropna(subset=['ns_sec_1'])
print(LNN_L_clean2.shape[0])

len_three_clean =len_three.merge(sectors.drop_duplicates(subset=['Postdist']), left_on='Postcode',right_on='Postdist').dropna(subset=['ns_sec_1'])
print(len_three_clean.shape[0])

len_two_clean = len_two.merge(sectors.drop_duplicates(subset=['Postdist']), left_on='Postcode',right_on='Postdist').dropna(subset=['ns_sec_1'])
print(len_two_clean.shape[0])

LNN_clean = LNN.merge(sectors_LNN, left_on='LNN', right_on='Postdist', how='left').dropna(subset=['ns_sec_1'])
print(LNN_clean.shape[0])


LLNN_temp_1 = LLNN.merge(sectors_LLNN, left_on='Postcode', right_on='Postdist', how='left').dropna(subset=['ns_sec_1'])
# 11723
LLNN_temp_2= LLNN.merge(sectors_LLN, left_on='Postcode', right_on='Postcode_nospace', how='left').dropna(subset=['ns_sec_1'])

# define user postal districts that could be LLN or LLNN
# can be used for exploratory analysis
LLN_LLNN = LLNN_temp_1.loc[LLNN_temp_1['PseudoUserId'].isin(LLNN_temp_2['PseudoUserId'])==True].drop_duplicates(subset=['PseudoUserId','Postdist'])

# LLN clean is in LLN_sectors but not LLNN sectors
LLN_clean = LLNN_temp_2.loc[LLNN_temp_2['PseudoUserId'].isin(LLNN_temp_1['PseudoUserId'])==False].drop_duplicates(subset=['PseudoUserId','Postdist'])

# LLNN clean is in LLNN_sectors but not LLN sectors
LLNN_clean = LLNN_temp_1.loc[LLNN_temp_1['PseudoUserId'].isin(LLNN_temp_2['PseudoUserId'])==False].drop_duplicates(subset=['PseudoUserId','Postdist'])

d1 = LLNL_clean[['PseudoUserId', 'Gender', 'Postcode_x', 'Year_Of_Birth',\
       'Age', 'Postdist','Latitude', 'Longitude', 'Easting',
       'Northing', 'Grid Ref', 'Postcodes', 'Active postcodes', 'Population',
       'Households', 'Built up area', 'postdist', 'ns_sec_1', 'ns_sec_1_per',
       'ns_sec_2', 'ns_sec_2_per', 'ns_sec_3', 'ns_sec_3_per', 'ns_sec_4',
       'ns_sec_4_per', 'ns_sec_5', 'ns_sec_5_per', 'ns_sec_6', 'ns_sec_6_per',
       'ns_sec_7', 'ns_sec_7_per', 'ns_sec_8', 'ns_sec_8_per',
       'ns_sec_notclass', 'ns_sec_notclass_per']]

d2 = LNL_clean[['PseudoUserId', 'Gender', 'Postcode_x', 'Year_Of_Birth',\
       'Age', 'Postdist','Latitude', 'Longitude', 'Easting',
       'Northing', 'Grid Ref', 'Postcodes', 'Active postcodes', 'Population',
       'Households', 'Built up area', 'postdist', 'ns_sec_1', 'ns_sec_1_per',
       'ns_sec_2', 'ns_sec_2_per', 'ns_sec_3', 'ns_sec_3_per', 'ns_sec_4',
       'ns_sec_4_per', 'ns_sec_5', 'ns_sec_5_per', 'ns_sec_6', 'ns_sec_6_per',
       'ns_sec_7', 'ns_sec_7_per', 'ns_sec_8', 'ns_sec_8_per',
       'ns_sec_notclass', 'ns_sec_notclass_per']]

d3 = LN_clean[['PseudoUserId', 'Gender', 'Postcode_x', 'Year_Of_Birth',\
       'Age', 'Postdist','Latitude', 'Longitude', 'Easting',
       'Northing', 'Grid Ref', 'Postcodes', 'Active postcodes', 'Population',
       'Households', 'Built up area', 'postdist', 'ns_sec_1', 'ns_sec_1_per',
       'ns_sec_2', 'ns_sec_2_per', 'ns_sec_3', 'ns_sec_3_per', 'ns_sec_4',
       'ns_sec_4_per', 'ns_sec_5', 'ns_sec_5_per', 'ns_sec_6', 'ns_sec_6_per',
       'ns_sec_7', 'ns_sec_7_per', 'ns_sec_8', 'ns_sec_8_per',
       'ns_sec_notclass', 'ns_sec_notclass_per']]
d4 = LNN_clean[['PseudoUserId', 'Gender', 'Postcode_x', 'Year_Of_Birth',\
       'Age', 'Postdist','Latitude', 'Longitude', 'Easting',
       'Northing', 'Grid Ref', 'Postcodes', 'Active postcodes', 'Population',
       'Households', 'Built up area', 'postdist', 'ns_sec_1', 'ns_sec_1_per',
       'ns_sec_2', 'ns_sec_2_per', 'ns_sec_3', 'ns_sec_3_per', 'ns_sec_4',
       'ns_sec_4_per', 'ns_sec_5', 'ns_sec_5_per', 'ns_sec_6', 'ns_sec_6_per',
       'ns_sec_7', 'ns_sec_7_per', 'ns_sec_8', 'ns_sec_8_per',
       'ns_sec_notclass', 'ns_sec_notclass_per']]
d5 = LLNN_clean[['PseudoUserId', 'Gender', 'Postcode_x', 'Year_Of_Birth',\
       'Age', 'Postdist','Latitude', 'Longitude', 'Easting',
       'Northing', 'Grid Ref', 'Postcodes', 'Active postcodes', 'Population',
       'Households', 'Built up area', 'postdist', 'ns_sec_1', 'ns_sec_1_per',
       'ns_sec_2', 'ns_sec_2_per', 'ns_sec_3', 'ns_sec_3_per', 'ns_sec_4',
       'ns_sec_4_per', 'ns_sec_5', 'ns_sec_5_per', 'ns_sec_6', 'ns_sec_6_per',
       'ns_sec_7', 'ns_sec_7_per', 'ns_sec_8', 'ns_sec_8_per',
       'ns_sec_notclass', 'ns_sec_notclass_per']]

d6 = LLN_clean[['PseudoUserId', 'Gender', 'Postcode_x', 'Year_Of_Birth',\
       'Age', 'Postdist','Latitude', 'Longitude', 'Easting',
       'Northing', 'Grid Ref', 'Postcodes', 'Active postcodes', 'Population',
       'Households', 'Built up area', 'postdist', 'ns_sec_1', 'ns_sec_1_per',
       'ns_sec_2', 'ns_sec_2_per', 'ns_sec_3', 'ns_sec_3_per', 'ns_sec_4',
       'ns_sec_4_per', 'ns_sec_5', 'ns_sec_5_per', 'ns_sec_6', 'ns_sec_6_per',
       'ns_sec_7', 'ns_sec_7_per', 'ns_sec_8', 'ns_sec_8_per',
       'ns_sec_notclass', 'ns_sec_notclass_per']]

d7 = len_two_clean[['PseudoUserId', 'Gender', 'Postcode_x', 'Year_Of_Birth',\
       'Age', 'Postdist','Latitude', 'Longitude', 'Easting',
       'Northing', 'Grid Ref', 'Postcodes', 'Active postcodes', 'Population',
       'Households', 'Built up area', 'postdist', 'ns_sec_1', 'ns_sec_1_per',
       'ns_sec_2', 'ns_sec_2_per', 'ns_sec_3', 'ns_sec_3_per', 'ns_sec_4',
       'ns_sec_4_per', 'ns_sec_5', 'ns_sec_5_per', 'ns_sec_6', 'ns_sec_6_per',
       'ns_sec_7', 'ns_sec_7_per', 'ns_sec_8', 'ns_sec_8_per',
       'ns_sec_notclass', 'ns_sec_notclass_per']]

d8 = len_three_clean[['PseudoUserId', 'Gender', 'Postcode_x', 'Year_Of_Birth',\
       'Age', 'Postdist','Latitude', 'Longitude', 'Easting',
       'Northing', 'Grid Ref', 'Postcodes', 'Active postcodes', 'Population',
       'Households', 'Built up area', 'postdist', 'ns_sec_1', 'ns_sec_1_per',
       'ns_sec_2', 'ns_sec_2_per', 'ns_sec_3', 'ns_sec_3_per', 'ns_sec_4',
       'ns_sec_4_per', 'ns_sec_5', 'ns_sec_5_per', 'ns_sec_6', 'ns_sec_6_per',
       'ns_sec_7', 'ns_sec_7_per', 'ns_sec_8', 'ns_sec_8_per',
       'ns_sec_notclass', 'ns_sec_notclass_per']]

d9 = LNN_L_clean[['PseudoUserId', 'Gender', 'Postcode_x', 'Year_Of_Birth',\
       'Age', 'Postdist','Latitude', 'Longitude', 'Easting',
       'Northing', 'Grid Ref', 'Postcodes', 'Active postcodes', 'Population',
       'Households', 'Built up area', 'postdist', 'ns_sec_1', 'ns_sec_1_per',
       'ns_sec_2', 'ns_sec_2_per', 'ns_sec_3', 'ns_sec_3_per', 'ns_sec_4',
       'ns_sec_4_per', 'ns_sec_5', 'ns_sec_5_per', 'ns_sec_6', 'ns_sec_6_per',
       'ns_sec_7', 'ns_sec_7_per', 'ns_sec_8', 'ns_sec_8_per',
       'ns_sec_notclass', 'ns_sec_notclass_per']]

d10 = LNN_L_clean2[['PseudoUserId', 'Gender', 'Postcode_x', 'Year_Of_Birth',\
       'Age', 'Postdist','Latitude', 'Longitude', 'Easting',
       'Northing', 'Grid Ref', 'Postcodes', 'Active postcodes', 'Population',
       'Households', 'Built up area', 'postdist', 'ns_sec_1', 'ns_sec_1_per',
       'ns_sec_2', 'ns_sec_2_per', 'ns_sec_3', 'ns_sec_3_per', 'ns_sec_4',
       'ns_sec_4_per', 'ns_sec_5', 'ns_sec_5_per', 'ns_sec_6', 'ns_sec_6_per',
       'ns_sec_7', 'ns_sec_7_per', 'ns_sec_8', 'ns_sec_8_per',
       'ns_sec_notclass', 'ns_sec_notclass_per']]

df_user_postcode = pd.concat([d1,d2,d3,d4,d5,d6,d7,d8,d9,d10])

df_user_postcode.shape[0]

df_user_postcode_count = df_user_postcode.groupby('Postdist').size().to_frame('N').reset_index()

df_user_postcode_count.shape[0]


#%% CLEAN ACTIVITIES
# check for any duplicated rows (all values exactly the same in selected rows)
duplicates = active.loc[active.duplicated(['PseudoUserId', 'Date', 'Distance_Travelled(m)','Activity_Duration(s)',
       'Steps', 'Average_Speed_(km_h)'])==True] #189198
print('Number of duplicates=', duplicates.shape[0] )
#%%
# drop duplicated rows
active_clean= active.drop_duplicates(['PseudoUserId', 'Date', 'Distance_Travelled(m)','Activity_Duration(s)',
       'Steps', 'Average_Speed_(km_h)'], keep='first')
print('Number of activities=', active_clean.shape[0] )
#%%    
# Remove activities with negative metrics (impossible)
active_clean = active_clean.drop(active_clean.loc[active_clean['Distance_Travelled(m)'] < 0].index) # 3
active_clean = active_clean.drop(active_clean.loc[active_clean['Activity_Duration(s)'] < 0].index) #4
print('Number of activities=', active_clean.shape[0] )    
#%%
# Convert Date string to datetime format
active_clean['Date'] = pd.to_datetime(active_clean['Date'])

# Replace Nan with none for time (some activities may not be timed)
active_clean['Activity_Duration(s)'] = active_clean['Activity_Duration(s)'].replace({pd.np.nan: None})

# calculate active minutes
active_clean['Activity_Duration(min)'] = active_clean['Activity_Duration(s)']/60

# calculate active minutes rounded (fitbit only recorded in minutes)
decimals=0
active_clean['Activity_Duration(r_min)'] = active_clean['Activity_Duration(min)'].apply(lambda x:round(x, decimals))
#%%
print('Number of activities=', active_clean.shape[0] ) 
#%%
# remove activities over 24 hours
active_clean = active_clean.drop(active_clean.loc[active_clean['Activity_Duration(min)']>=1440].index) #10216
print('Number of activities=', active_clean.shape[0] ) 

# CALCULATE 99 PERCENTILE FOR CYCLING DISTANCE AND SPEED
cycling = active_clean.loc[active_clean['Activity_Type']== 'cycling']
print('Cycling 99 percentile distance travelled (km)=',(cycling['Distance_Travelled(m)'].quantile(0.99))/1000)
cdq= cycling['Distance_Travelled(m)'].quantile(0.99)
print('Cycling 99 percentile average speed (km/h)=',cycling['Average_Speed_(km_h)'].quantile(0.99))
csq= cycling['Average_Speed_(km_h)'].quantile(0.99)

running = active_clean.loc[active_clean['Activity_Type']== 'running']
print('Running 99 percentile distance travelled (km)=',(running['Distance_Travelled(m)'].quantile(0.99))/1000)
rdq = running['Distance_Travelled(m)'].quantile(0.99)
print('Running 99 percentile average speed (km/h) =',running['Average_Speed_(km_h)'].quantile(0.99))
rsq = running['Average_Speed_(km_h)'].quantile(0.99)

#%% clean activities to be within 99th percentile(s) for speed and distance dependent on activity type 
print('Number of activities > cycling 99 percentile distance travelled =',cycling.loc[cycling['Distance_Travelled(m)']>cdq].shape[0])
active_clean = active_clean.drop(cycling.loc[cycling['Distance_Travelled(m)']>cdq].index)
print('Number of activities=', active_clean.shape[0] )

cycling = active_clean.loc[active_clean['Activity_Type']== 'cycling']
print('Number of activities > cycling 99 percentile average speed =',cycling.loc[cycling['Average_Speed_(km_h)']>csq].shape[0])
active_clean = active_clean.drop(cycling.loc[cycling['Average_Speed_(km_h)']>csq].index)
print('Number of activities=', active_clean.shape[0])

print('Number of activities > running 99 percentile distance travelled =', running.loc[running['Distance_Travelled(m)']>rdq].shape[0])
active_clean = active_clean.drop(running.loc[running['Distance_Travelled(m)']>rdq].index)
print('Number of activities=', active_clean.shape[0] )

running = active_clean.loc[active_clean['Activity_Type']== 'running']
print('Number of activities > running 99 percentile average speed =',running.loc[running['Average_Speed_(km_h)']>rsq].shape[0])
active_clean = active_clean.drop(running.loc[running['Average_Speed_(km_h)']>rsq].index)
print('Number of activities=', active_clean.shape[0] )
#%%
# Repeat cleaning for MOVES activity type:
    # move activities could be misclassified running, cycling etc. activities

move = active_clean.loc[(active_clean['Activity_Type']== 'other') | (active_clean['Activity_Type']== 'move')]
print(' Number of move activities =', move.shape[0])

print(' Number of move activities with no average speed=', move.loc[move['Average_Speed_(km_h)'].isnull()].shape[0])
# Moves activities suspected to be cycling have an average speed greater than the 99 percentile for running
move_sus_cycling = move.loc[move['Average_Speed_(km_h)']>rsq]
print('Number of move activities suspected to be cycling from average speed=', move_sus_cycling.shape[0])
# Moves activities suspected to be running or other activities have an average speed less than or equal to the 99 percentile for running
move_sus_run = move.loc[move['Average_Speed_(km_h)']<=rsq]
print('Number of move activities suspected to be running (or other) from average speed=', move_sus_run.shape[0])
#
print(' Number of move activities suspected to be cycling >99 percentile for cycling distance travelled)=',move_sus_cycling.loc[move_sus_cycling['Distance_Travelled(m)']>=cdq].shape[0])
active_clean = active_clean.drop(move_sus_cycling.loc[move_sus_cycling['Distance_Travelled(m)']>=cdq].index)
print('Number of activities=', active_clean.shape[0] )

# redefine move_sus_cycle from new cleaned data
move = active_clean.loc[active_clean['Activity_Type']== 'move']
move_sus_cycling = move.loc[move['Average_Speed_(km_h)']>rsq]
print(' Number of move activities suspected to be cycling >99 percentile for cycling average speed)=',move_sus_cycling.loc[move_sus_cycling['Average_Speed_(km_h)']>=csq].shape[0])
active_clean = active_clean.drop(move_sus_cycling.loc[move_sus_cycling['Average_Speed_(km_h)']>=csq].index)
print('Number of activities=', active_clean.shape[0] )

#
print(' Number of move activities suspected to be running >99 percentile for running distance travelled)=',move_sus_run.loc[move_sus_run['Distance_Travelled(m)']>=rdq].shape[0])
active_clean = active_clean.drop(move_sus_run.loc[move_sus_run['Distance_Travelled(m)']>=rdq].index)
print('Number of activities=', active_clean.shape[0] )

# redefine move_sus_run from new cleaned data
move = active_clean.loc[active_clean['Activity_Type']== 'move']
move_sus_run = move.loc[move['Average_Speed_(km_h)']<=rsq]
print(' Number of move activities suspected to be running >99 percentile for running average speed)=',move_sus_run.loc[move_sus_run['Average_Speed_(km_h)']>=rsq].shape[0])
active_clean = active_clean.drop(move_sus_run.loc[move_sus_run['Average_Speed_(km_h)']>=rsq].index)
print('Number of activities=', active_clean.shape[0] )

# re-clean activities to be within 99th percentile(s) for speed and distance dependent on activity type 
active_clean = active_clean.drop(active_clean.loc[active_clean['Average_Speed_(km_h)']>csq].index) #1233
print('Number of activities=', active_clean.shape[0] )
active_clean = active_clean.drop(active_clean.loc[active_clean['Distance_Travelled(m)']>cdq].index) #73
print('Number of activities=', active_clean.shape[0] )
active_clean = active_clean.drop(active_clean.loc[active_clean['Steps']>active_clean['Steps'].quantile(0.999)].index)  #7672
print('Number of activities=', active_clean.shape[0] )

#%%
# clean dates to be for just 2016
active_clean = active_clean.loc[active_clean['Date']<='2016-12-31 00:00:00']

# include only users with 7 days or more activity
# get number of dates on which each user recorded activity
nunique_actve_clean_user = pd.DataFrame(active_clean.groupby('PseudoUserId')['Date'].nunique()).reset_index()
# define users with 7 or more days activity
users_7days = nunique_actve_clean_user['PseudoUserId'].loc[nunique_actve_clean_user['Date'] >=7]
# define users with less than 7 days activity
users_no7days = nunique_actve_clean_user['PseudoUserId'].loc[nunique_actve_clean_user['Date']  <7]

# subset cleaned activity to only include users with 7 or more days of activity
active_clean = active_clean[active_clean.PseudoUserId.isin(users_7days)]

print('Number of activities=', active_clean.shape[0] )
#%%
active_clean.to_csv("output_files/cleaned_activity_data.csv")

# merge cleaned activity and cleaned user data to get cleaned activities from valid users
all_act = pd.merge(active_clean, user, on='PseudoUserId', how='inner')
print('Number of activities (for users with >= 7 days of activity and valid user info) =', all_act.shape[0] )

all_user_unique =  pd.DataFrame(all_act.groupby(["PseudoUserId"])["Date"].nunique()).reset_index()
print('Number of users with valid activity=',all_user_unique.shape[0])

# Use IDs of valid users with cleaned activities to subset valid users to only include those with sufficent cleaned activity. 
all_users= user.loc[user["PseudoUserId"].isin(all_user_unique["PseudoUserId"])]

# save files to csv
all_users.to_csv("output_files/all_user.csv")
all_act.to_csv("output_files/all_act.csv")

# repeat above for just users with valid postcode: get all activities
post_act = pd.merge(active_clean, df_user_postcode, on='PseudoUserId', how='inner')
print('Number of activities (for users with >= 7 days of activity and valid user info and known postocde) =', post_act.shape[0] )

# repeat above for just users with valid postcode: get unique users
postcode_user_unique=  pd.DataFrame(post_act.groupby(["PseudoUserId"])["Date"].nunique()).reset_index()
print('Number of users with valid postcode and activity=', postcode_user_unique.shape[0] )

# repeat above for just users with valid postcode: get all users
post_users = df_user_postcode.loc[df_user_postcode['PseudoUserId'].isin(postcode_user_unique['PseudoUserId'])]

# save files to csv
post_users.to_csv("output_files/post_user.csv")
post_act.to_csv("output_files/post_act.csv")

print('time taken:',datetime.now()-startTime)
