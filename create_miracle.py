import sqlite3
import pandas as pd
import numpy as np
from re import search

conn = sqlite3.connect("""/Users/mattkirby/Desktop/miracle_messages.sqlite3""")
curs = conn.cursor()

csv = '/Users/mattkirby/Desktop/Master MM Volunteers - MM Volunteers.csv'
df = pd.read_csv(csv)

# Data Cleaning
def matt_clean():

    # First Name
    df['First Name'] = df['First Name'].str.title()
    df['First Name'] = df['First Name'].fillna('UNKNOWN')

    # Last Name
    df['Last Name'] = df['Last Name'].str.title()
    df['Last Name'] = df['Last Name'].fillna('UNKNOWN')

    #Phone Numbers
    # Remove all +, -, and () from phone numbers
    df['Phone'] = df['Phone'].astype(str).str.replace('[+,\-,(,),\ ]', '')
    df['Phone'] = df['Phone'].fillna('UNKNOWN')
    # Format the phone numbers _(___)___-____
    #df['Phone'] = df['Phone'].astype(str).apply(lambda x:
            #x[:-10] + '(' + x[-10:-7] + ')' + x[-7:-4] + '-' + x[-4:])
    # Replace nan values with Unknown
    #df['Phone'] = df['Phone'].replace({'()-nan':'UNKNOWN'})
    # Fill in numbers found in comments
    df.at[3245, 'Phone'] = '(207)310-0383'
    df.at[4445, 'Phone'] = '(778)386-3122'
    df.at[6841, 'Phone'] = '(858)212-7503'
    df.at[4675, 'Phone'] = '(228)490-5027'

states = {
        'AK': 'Alaska',
        'AL': 'Alabama',
        'AR': 'Arkansas',
        'AS': 'American Samoa',
        'AZ': 'Arizona',
        'CA': 'California',
        'CO': 'Colorado',
        'CT': 'Connecticut',
        'DC': 'District of Columbia',
        'DE': 'Delaware',
        'FL': 'Florida',
        'GA': 'Georgia',
        'GU': 'Guam',
        'HI': 'Hawaii',
        'IA': 'Iowa',
        'ID': 'Idaho',
        'IL': 'Illinois',
        'IN': 'Indiana',
        'KS': 'Kansas',
        'KY': 'Kentucky',
        'LA': 'Louisiana',
        'MA': 'Massachusetts',
        'MD': 'Maryland',
        'ME': 'Maine',
        'MI': 'Michigan',
        'MN': 'Minnesota',
        'MO': 'Missouri',
        'MP': 'Northern Mariana Islands',
        'MS': 'Mississippi',
        'MT': 'Montana',
        'NA': 'National',
        'NC': 'North Carolina',
        'ND': 'North Dakota',
        'NE': 'Nebraska',
        'NH': 'New Hampshire',
        'NJ': 'New Jersey',
        'NM': 'New Mexico',
        'NV': 'Nevada',
        'NY': 'New York',
        'OH': 'Ohio',
        'OK': 'Oklahoma',
        'OR': 'Oregon',
        'PA': 'Pennsylvania',
        'PR': 'Puerto Rico',
        'RI': 'Rhode Island',
        'SC': 'South Carolina',
        'SD': 'South Dakota',
        'TN': 'Tennessee',
        'TX': 'Texas',
        'UT': 'Utah',
        'VA': 'Virginia',
        'VI': 'Virgin Islands',
        'VT': 'Vermont',
        'WA': 'Washington',
        'WI': 'Wisconsin',
        'WV': 'West Virginia',
        'WY': 'Wyoming'}

can_provinces = {'AB':'Alberta',
                 'BC':'British Columbia',
                 'MB':'Manitoba',
                 'NB':'New Brunswick',
                 'NL':'Newfoundland',
                 'NS':'Nova Scotia',
                 'ON':'Ontario',
                 'PE':'Prince Edward Island',
                 'QC':'Quebec',
                 'SK':'Saskatchewan',
                 'NT':'Northwest Territories',
                 'NU':'Nunavut',
                 'YT':'Yukon'}

US_cities = ['Boston', 'Los Angeles', 'San Diego', 'San Francisco',
             'Salt Lake City', 'Chicago', 'New York City', 'Dallas',
             'Cincinnati', 'Baltimore', 'Seattle', 'Philadelphia',
             'New Orleans', 'Nashville', 'Pittsburgh', 'Memphis',
             'Charlottesville', 'Knoxville', 'Miami']

countries = {'USA':'United States',
             'US':'United States',
             'United States of America':'United States',
             'CAN':'Canada',
             'UK':'United Kingdom',
             'AU':'Australia',
             'AUS':'Australia',
             'GB':'United Kingdom',
             'DE':'Germany',
             'FR':'France'}

def strip_nan():
  for col in ['City','Stateprovince','Country','Full Address']:
    df[col] = df[col].str.strip()
    df[col] = df[col].str.strip('.')
    df.loc[df[col] == '', col] = np.NaN

def try_join(l):
  try:
    return ' '.join(map(str, l))
  except TypeError:
    return np.NaN

def city_state_country_clean():
  strip_nan()

  #Pick out States from full address, fill matching empty 'Stateprovince' slot
  #Convert State Abbreviations to Full Name
  for abr in states:
    df.loc[(df['Full Address'].str.contains(states[abr]) &
            df['Stateprovince'].isnull()) |
           (df['Stateprovince'].str.lower() == abr.lower()),'Stateprovince'] = states[abr]
    df.loc[df['Stateprovince'].isnull() & df['Full Address'].str.contains(abr) &
           df['Full Address'].str.contains(states[abr]),
           'Stateprovince'] = states[abr]
    df.loc[(df['Country'] == 'USA') & (df['Stateprovince'].isnull()) &
           (df['Full Address'].str.contains(abr)) & (~df['City'].isnull()),
           'Stateprovince'] = states[abr]
    df.loc[(df['Stateprovince'].isnull()) &
           df['City'].str.lower().str.endswith(' '+ abr.lower()), 'Stateprovince'] = states[abr]
    df.loc[(df['Stateprovince'].isnull()) &
           df['Full Address'].str.lower().str.endswith(' '+abr.lower()), 'Stateprovince'] = states[abr]

  #Same thing but with Canadian Provinces
  for abr in can_provinces:
    df.loc[(df['Full Address'].str.contains(can_provinces[abr]) &
           df['Stateprovince'].isnull()) |
           (df['Stateprovince'].str.lower() == abr.lower()),'Stateprovince'] = can_provinces[abr]
    df.loc[df['Stateprovince'].isnull() & df['Full Address'].str.contains(abr) &
           df['Full Address'].str.contains(can_provinces[abr]),
           'Stateprovince'] = can_provinces[abr]
    df.loc[(df['Stateprovince'].isnull()) &
           df['City'].str.lower().str.endswith(' '+ abr.lower()), 'Stateprovince'] = can_provinces[abr]
    df.loc[(df['Stateprovince'].isnull()) &
           df['Full Address'].str.lower().str.endswith(' '+abr.lower()), 'Stateprovince'] = can_provinces[abr]

  #Find Major US Cities with Unique Names, fill in city and country
  for city in US_cities:
    df.loc[df['Full Address'].str.contains(city) &
           df['City'].isnull(), 'City'] = city
    df.loc[df['Full Address'].str.contains(city) &
           df['Country'].isnull(), 'Country'] = 'USA'

  #Get City, and if statprovince is empty, dump everything after first comma there
  temp = df['City'].str.split(',')
  df['City'] = temp.str[0]
  mask = df['Stateprovince'].isnull()
  df.loc[mask, 'Stateprovince'] = temp.loc[mask].str[1:]
  df.loc[mask, 'Stateprovince'] = [try_join(l) for l in
                                   df.loc[mask, 'Stateprovince']]
  strip_nan()

  #Set Country of US States to 'USA'
  for abr in states:
    df.loc[df['Stateprovince'].str.lower() == states[abr].lower(), 'Country'] = 'USA'

  #Set Country of CAN Provinces to 'Canada'
  for abr in can_provinces:
    df.loc[df['Stateprovince'].str.lower() == can_provinces[abr].lower(), 'Country'] = 'Canada'

  #US Postal Codes from Full Address
  for i in df['Full Address'].index:
    if ((df[df.index == i]['ZIP'].apply(type).any() != str) &
        (df[df.index == i]['Full Address'].apply(type).any() == str)):
      if search('\d{5}', df.loc[i, 'Full Address']):
        df.loc[i, 'ZIP'] = search('\d{5}', df.loc[i, 'Full Address']).group(0)

  #Convert abbreviations to full country name
  for abr in countries:
    df.loc[df['Country'].str.lower() == abr.lower(), 'Country'] = countries[abr]
    df.loc[df['Stateprovince'].str.lower() == abr.lower(), 'Country'] = countries[abr]
    df.loc[((df['City'].str.lower() == abr.lower()) |
           (df['City'].str.lower() == countries[abr].lower())),
           'Country'] = countries[abr]
    df.loc[((df['Stateprovince'].str.lower() == abr.lower()) |
           (df['Stateprovince'].str.lower() == countries[abr].lower())),
           'Country'] = countries[abr]
    df.loc[((df['Full Address'].str.lower() == abr.lower()) |
           (df['Full Address'].str.lower() == countries[abr].lower())),
           'Country'] = countries[abr]

  strip_nan()

def location_clean():
  city_state_country_clean()

  #City
  df.loc[((df.index == 1666) | (df.index == 1668)), 'City'] = 'Dublin'
  df.loc[df.index == 711, 'City'] = 'Bath'
  df.loc[df.index == 4406, 'City'] = 'Sydney'
  df.loc[df.index == 2276, 'City'] = 'Liverpool'
  df.loc[df.index == 2949, 'City'] = 'New Castle'
  df.loc[df.index == 2682, 'City'] = 'Minneapolis'
  df.loc[df.index == 730, 'City'] = 'Belgrade'
  df.loc[df.index == 2793, 'City'] = 'Bridgeport'
  df.loc[df.index == 733, 'City'] = 'Belton'
  df.loc[df.index == 912, 'City'] = 'Buffalo'
  df.loc[df.index == 929, 'City'] = 'Byron Bay'
  df.loc[df.index == 1483, 'City'] = 'Columbus'
  df.loc[df.index == 1571, 'City'] = 'Daytona Beach'
  df.loc[df.index == 1585, 'City'] = 'Denton'
  df.loc[df.index == 1653, 'City'] = 'Detroit'
  df.loc[df.index == 1735, 'City'] = 'Fallbrook'
  df.loc[df.index == 2039, 'City'] = 'Houston'
  df.loc[df.index == 2187, 'City'] = 'Monroe'
  df.loc[df.index == 2243, 'City'] = 'Las Vegas'
  df.loc[df.index == 2721, 'City'] = 'Montreal'
  df.loc[df.index == 2784, 'City'] = 'Nelson'
  df.loc[df.index == 2803, 'City'] = 'New Orleans'
  df.loc[df.index == 2810, 'City'] = 'Orlando'
  df.loc[df.index == 2987, 'City'] = 'Philadelphia'
  df.loc[df.index == 3015, 'City'] = 'Fremont'
  df.loc[df.index == 3184, 'City'] = 'Orange County'
  df.loc[df.index == 3451, 'City'] = 'Raleigh'
  df.loc[df.index == 3571, 'City'] = 'Salt Lake City'
  df.loc[((df.index == 3613) | (df.index == 4517)), 'City'] = 'San Diego'
  df.loc[df.index == 3958, 'City'] = 'Santa Cruz'
  df.loc[df.index == 3978, 'City'] = 'Santa Rosa'
  df.loc[df.index == 4034, 'City'] = 'Seattle'
  df.loc[df.index == 4108, 'City'] = 'Lake Tahoe'
  df.loc[df.index == 4182, 'City'] = 'Stockholm'
  df.loc[df.index == 4183, 'City'] = 'Stony Brook'
  df.loc[df.index == 4186, 'City'] = 'Stuart'
  df.loc[df.index == 4243, 'City'] = 'Tampa'
  df.loc[df.index == 4334, 'City'] = 'Spokane'
  df.loc[df.index == 4413, 'City'] = 'Brighton'
  df.loc[df.index == 4497, 'City'] = 'Victoria'
  df.loc[((df.index == 4506) | (df.index == 2983)), 'City'] = 'Virginia Beach'
  df.loc[df.index == 4548, 'City'] = 'Warrington'
  df.loc[((df.index == 4571) | (df.index == 3434)), 'City'] = 'Washington'
  df.loc[df.index == 4571, 'City'] = 'Williamsport'
  df.loc[df.index == 4668, 'City'] = 'York'
  df.loc[df.index == 4678, 'City'] = 'Stanford'
  df.loc[df.index == 4716, 'City'] = 'Arlington'
  df.loc[df.index == 5896, 'City'] = 'Burlingame'
  df.loc[df.index == 6892, 'City'] = 'Oakland'
  df.loc[df.index == 212, 'City'] = 'Monrovia'
  df.loc[df.index == 333, 'City'] = 'Pittsburgh'
  df.loc[df.index == 349, 'City'] = 'Memphis'
  df.loc[df.index == 355, 'City'] = 'Toledo'
  df.loc[df.index == 539, 'City'] = 'Alexandria'
  df.loc[df.index == 631, 'City'] = 'Nevada City'
  df.loc[df.index == 699, 'City'] = 'Baltimore'
  df.loc[df.index == 702, 'City'] = 'Bangor'
  df.loc[df.index == 837, 'City'] = 'Boulder City'
  df.loc[df.index == 852, 'City'] = 'Brasilia'
  df.loc[df.index == 903, 'City'] = 'Bryan'
  df.loc[df.index == 909, 'City'] = 'Buenos Aires'
  df.loc[df.index == 924, 'City'] = 'Burlington'
  df.loc[df.index == 1283, 'City'] = 'Cambridge'
  df.loc[df.index == 1293, 'City'] = 'Canton'
  df.loc[df.index == 1308, 'City'] = 'Carrollton'
  df.loc[df.index == 1328, 'City'] = 'Knoxville'
  df.loc[df.index == 1344, 'City'] = 'Charlottesville'
  df.loc[df.index == 1348, 'City'] = 'Chelmsford'
  df.loc[df.index == 1453, 'City'] = 'Collierville'
  df.loc[df.index == 1839, 'City'] = 'Frankfurt'
  df.loc[df.index == 1916, 'City'] = 'Aachen'
  df.loc[df.index == 4665, 'City'] = 'Novato'
  df.loc[df.index == 3531, 'City'] = 'Wallaceburg'
  df.loc[df.index == 2523, 'City'] = 'Cork'
  df.loc[df.index == 2100, 'City'] = 'Ipswich'
  df.loc[df.index == 4612, 'City'] = 'Roby Hill'
  df.loc[df.index == 1980, 'City'] = 'New London'
  df.loc[df.index == 1995, 'City'] = 'Hesperia'
  df.loc[df.index == 1995, 'City'] = 'Kampala'
  df.loc[df.index == 2730, 'City'] = 'Moscow'
  df.loc[df.index == 1917, 'City'] = 'Saarbrucken'
  df.loc[((df.index == 3130) | (df.index == 1430)), 'City'] = 'Cleveland'
  df.loc[df.index == 3170, 'City'] = 'Huntington'
  df.loc[df.index == 3205, 'City'] = 'Huntington'
  df.loc[df.index == 4092, 'City'] = 'Singapore'
  df.loc[df.index == 2127, 'City'] = 'Johannesburg'
  df.loc[df.index == 319, 'City'] = 'New Mexico'
  df.loc[df.index == 2138, 'City'] = 'Kamloomps'
  df.loc[df.index == 3982, 'City'] = 'Santa Rosa'
  df.loc[df.index == 4999, 'City'] = 'Escondido'
  df.loc[df.index == 13, 'City'] = 'Sousse'
  df.loc[((df.index == 4842) | (df.index == 1321)), 'City'] = 'Paris'
  df.loc[((df.index == 1469) | (df.index == 1470)), 'City'] = 'Columbia'
  df.loc[((df.index == 839) | (df.index == 840)), 'City'] = 'Boulder'
  df.loc[((df.index == 1289) | (df.index == 4449)), 'City'] = 'Vancouver'
  df.loc[((df.index == 3223) | (df.index == 3437)), 'City'] = 'Ottawa'
  df.loc[((df.index == 2631) | (df.index == 2632)), 'City'] = 'Miami'

  mask = df['Stateprovince'].isnull() & df['Full Address'].str.contains('Sydney')
  df.loc[mask, 'City'] = 'Sydney'
  df.loc[mask, 'Stateprovince'] = 'New South Wales'
  df.loc[mask, 'Country'] = 'Australia'

  df.loc[~df['City'].isnull() &
         df['Full Address'].str.contains('London'), 'City'] = 'London'
  df.loc[df['City'].isnull() &
         df['Full Address'].str.contains('London'), 'City'] = 'London'
  df.loc[((df.index == 2333) | (df.index == 708) | (df.index == 4327)), 'City'] = 'London'
  London = [2333, 708, 4327, 2285, 2316, 2332, 2333]
  for index in London:
    df.loc[df.index == index, 'City'] = 'London'

  nan = [2007, 2008, 869, 1709, 1710]
  for index in nan:
    df.loc[df.index == index, 'City'] = np.NaN
  Nashville = [278, 2758, 2759, 2760]
  for index in Nashville:
    df.loc[df.index == index, 'City'] = 'Nashville'

  Portland = [412]
  for index in Portland:
    df.loc[df.index == index, 'City'] = 'Portland'

  LA = [2357, 2360, 189, 251]
  for index in LA:
    df.loc[df.index == index, 'City'] = 'Los Angeles'

  Chicago = [1400, 1403, 2263, 1948, 3106, 4185]
  for index in Chicago:
    df.loc[df.index == index, 'City'] = 'Chicago'

  Boston = [2957, 827, 1495]
  for index in Boston:
    df.loc[df.index == index, 'City'] = 'Boston'

  Dallas = [1270, 2055, 72]
  for index in Dallas:
    df.loc[df.index == index, 'City'] = 'Dallas'

  df.loc[df['City'].isnull() &
         df['Full Address'].str.contains('Toronto')] = 'Toronto'
  Toronto = [4286, 4310, 4315, 4321]
  for index in Toronto:
    df.loc[df.index == index, 'City'] = 'Toronto'

  SF = [3244, 3673, 3674, 4419, 4420, 7059, 7066, 155]
  for index in SF:
    df.loc[df.index == index, 'City'] = 'San Francisco'

  NYC = [2883, 895, 2936, 3002, 3089, 891, 896, 1968, 2769]
  for index in NYC:
    df.loc[df.index == index, 'City'] = 'New York City'

  #State
  df.loc[df.index == 1986, 'Stateprovince'] = 'Puerto Rico'
  df.loc[df.index == 68, 'Stateprovince'] = 'Gard'
  df.loc[df.index == 5389, 'Stateprovince'] = 'Vienne'
  df.loc[df.index == 5850, 'Stateprovince'] = 'Vence'
  df.loc[df.index == 674, 'Stateprovince'] = 'Provence'
  df.loc[df.index == 2505, 'Stateprovince'] = 'Auvergne'
  df.loc[df.index == 4828, 'Stateprovince'] = 'Bordeaux'
  df.loc[df.index == 4905, 'Stateprovince'] = 'Manche'
  df.loc[df.index == 2682, 'Stateprovince'] = 'Minnesota'
  df.loc[df.index == 1917, 'Stateprovince'] = 'Saarland'
  df.loc[df.index == 730, 'Stateprovince'] = 'Belgrade'
  df.loc[df.index == 4337, 'Stateprovince'] = 'Nova Scotia'
  df.loc[df.index == 4609, 'Stateprovince'] = 'Alberta'
  df.loc[df.index == 4655, 'Stateprovince'] = 'Manitoba'
  df.loc[df.index == 319, 'Stateprovince'] = 'New Mexico'
  df.loc[((df.index == 5464) | (df.index == 5834)), 'Stateprovince'] = 'Occitanie'
  df.loc[((df.index == 4846) | (df.index == 4847)), 'Stateprovince'] = 'Cher'
  df.loc[((df.index == 2557) | (df.index == 4111) | (df.index == 5199)), 'Stateprovince'] = 'New Jersey'
  df.loc[((df.index == 839) | (df.index == 3432)), 'Stateprovince'] = 'Colorado'
  df.loc[((df.index == 4404) | (df.index == 327)), 'Stateprovince'] = 'Victoria'
  df.loc[((df.index == 1653) | (df.index == 2743)), 'Stateprovince'] = 'Michigan'
  df.loc[df.index == 2187, 'Stateprovince'] = 'New Hampshire'
  df.loc[((df.index == 2721) | (df.index == 2722)), 'Stateprovince'] = 'Quebec'
  df.loc[df.index == 3170, 'Stateprovince'] = 'West Virginia'
  df.loc[df.index == 4182, 'Stateprovince'] = 'Uppland'
  df.loc[df.index == 4413, 'Stateprovince'] = 'Sussex'
  df.loc[df.index == 2100, 'Stateprovince'] = 'Suffolk'
  df.loc[df.index == 1348, 'Stateprovince'] = 'Essex'
  df.loc[df.index == 4571, 'Stateprovince'] = 'District of Columbia'
  df.loc[df.index == 702, 'Stateprovince'] = 'Maine'
  df.loc[df.index == 852, 'Stateprovince'] = 'Central West'
  df.loc[df.index == 909, 'Stateprovince'] = 'Buenos Aires'
  df.loc[((df.index == 924) | (df.index == 2999)), 'Stateprovince'] = 'Vermont'
  df.loc[((df.index == 1666) | (df.index == 1668)), 'Stateprovince'] = 'Leinster'
  df.loc[df.index == 1802, 'Stateprovince'] = 'Georgia'
  df.loc[df.index == 1839, 'Stateprovince'] = 'Hesse'
  df.loc[((df.index == 4007) | (df.index == 4187) | (df.index == 4599)), 'Stateprovince'] = 'Wisconsin'
  df.loc[df.index == 1860, 'Stateprovince'] = 'South Carolina'
  df.loc[df.index == 1910, 'Stateprovince'] = 'Geneva'
  df.loc[df.index == 2523, 'Stateprovince'] = 'Munster'
  df.loc[df.index == 4092, 'Stateprovince'] = 'Singapore'
  df.loc[df.index == 2730, 'Stateprovince'] = 'Central'
  df.loc[df.index == 2127, 'Stateprovince'] = 'Guateng'
  df.loc[df.index == 476, 'Stateprovince'] = 'Scotland'
  df.loc[df.index == 3, 'Stateprovince'] = 'Indiana'
  df.loc[df.index == 1965, 'Stateprovince'] = 'Hamburg'
  df.loc[df.index == 1971, 'Stateprovince'] = 'Lower Saxony'
  df.loc[df.index == 138, 'Stateprovince'] = 'Tbilisi'
  df.loc[df.index == 3447, 'Stateprovince'] = 'North Carolina'
  df.loc[df.index == 2166, 'Stateprovince'] = 'Wakiso District'
  df.loc[df.index == 1916, 'Stateprovince'] = 'North Rhine-Westphalia'
  df.loc[df.index == 2208, 'Stateprovince'] = 'Zurich'
  df.loc[df.index == 39, 'Stateprovince'] = 'Canberra'
  df.loc[df.index == 13, 'Stateprovince'] = 'Sousse'
  df.loc[((df.index == 1483) | (df.index == 355) | (df.index == 3130)), 'Stateprovince'] = 'Ohio'
  df.loc[((df.index == 4034) | (df.index == 4334) | (df.index == 4014)), 'Stateprovince'] = 'Washington'
  df.loc[((df.index == 2187) | (df.index == 2803)), 'Stateprovince'] = 'Louisiana'
  df.loc[((df.index == 3433) | (df.index == 2746)), 'Stateprovince'] = 'Maharashtra'
  df.loc[df['City'] == 'Montreal','Stateprovince'] = 'Quebec'
  df.loc[df['City'] == 'Barcelona','Stateprovince'] = 'Barcelona'
  df.loc[df['City'] == 'Paris','Stateprovince'] = 'Paris'

  Queensland = [1920, 4199, 859, 860, 861, 371]
  for index in Queensland:
    df.loc[df.index == index, 'Stateprovince'] = 'Queensland'

  Maryland = [699, 4717, 2709, 4417]
  for index in Maryland:
    df.loc[df.index == index, 'Stateprovince'] = 'Maryland'

  Oregon = [4136, 412]
  for index in Oregon:
    df.loc[df.index == index, 'Stateprovince'] = 'Oregon'

  Connecticut = [1561, 2793, 1922, 1980, 4645]
  for index in Connecticut:
    df.loc[df.index == index, 'Stateprovince'] = 'Connecticut'

  df.loc[df['City'] == 'Chicago', 'Stateprovince'] = 'Illinois'
  Illinois = [1400, 1403, 2263, 1948, 3106, 4185]
  for index in Illinois:
    df.loc[df.index == index, 'Stateprovince'] = 'Illinois'

  df.loc[(df['City'] == 'London') &
         ((df['Full Address'].str.contains('England')) |
          df['Full Address'].str.contains('UK') |
          df['Full Address'].str.contains('United Kingdom')),
         'Stateprovince'] = 'England'
  df.loc[((df['City'] == 'London') | (df['City'] == 'Brighton') |
          (df['City'] == 'Birmingham') | (df['City'] == 'Bristol') |
          (df['City'] == 'Cornwall') | (df['City'] == 'Manchester')) &
         (df['Country'] == 'United Kingdom'), 'Stateprovince'] = 'England'
  England = [0, 2326, 2327, 2328, 2329, 2330, 2331, 4117, 4118, 4400, 4612, 2316,
            5139, 1713, 2332, 2333, 137, 170, 370, 372, 769, 1349, 1410, 1433,
             1515, 1727, 4086, 711, 2276, 2949]
  for index in England:
    df.loc[df.index == index, 'Stateprovince'] = 'England'

  NSW = [929, 4224, 4225, 4226, 4211, 173, 4406]
  for index in NSW:
    df.loc[df.index == index, 'Stateprovince'] = 'New South Wales'

  Massachusetts = [2957, 827, 1495]
  for index in Massachusetts:
    df.loc[df.index == index, 'Stateprovince'] = 'Massachusetts'

  Arkansas = [1826, 1269, 1740]
  for index in Arkansas:
    df.loc[df.index == index, 'Stateprovince'] = 'Arkansas'

  Utah = [3430, 3571, 1529, 3572]
  for index in Utah:
    df.loc[df.index == index, 'Stateprovince'] = 'Utah'

  df.loc[df['City'] == 'Nashville', 'Stateprovince'] = 'Tennessee'
  Tennessee = [278, 349, 1328, 1344, 1453, 2604, ]
  for index in Tennessee:
    df.loc[df.index == index, 'Stateprovince'] = 'Tennessee'

  Virginia = [4506, 4716, 539, 2983]
  for index in Virginia:
    df.loc[df.index == index, 'Stateprovince'] = 'Virginia'

  Pennsylvania = [2987, 4605, 4668, 333, 3352, 3353, 3284, ]
  for index in Pennsylvania:
    df.loc[df.index == index, 'Stateprovince'] = 'Pennsylvania'

  df.loc[df['City'] == 'Toronto', 'Stateprovince'] = 'British Columbia'
  BC = [1289, 2784, 4449, 4497, 2748, 2749, 3344, 4202, 4453, 4456,
        4457, 4458, 4459, 4460, 4461, 4462, 4463, 4464, 4465, 4466, 4467,
        4468, 4469, 4470, 4471, 4472, 4473, 4477, 4498, 4499, 4500, 2138, 4450]
  for index in BC:
    df.loc[df.index == index, 'Stateprovince'] = 'British Columbia'

  df.loc[df['Stateprovince'].isnull() &
         df['Full Address'].str.contains('Ontario')] = 'Ontario'
  Ontario = [3223, 3437, 4286, 4310, 4315, 4321, 2652, 1956, 2166, 3219, 3225,
             3224, 3230, 3531, 4269, 4271, 4314, 4316, 4318, 4319, 4320, 4647,
             4322, 4323, 4324, 4325]
  for index in Ontario:
    df.loc[df.index == index, 'Stateprovince'] = 'Ontario'

  Nevada = [2243, 3479, 4108, ]
  for index in Nevada:
    df.loc[df.index == index, 'Stateprovince'] = 'Nevada'

  Florida = [1571, 2599, 2631, 2632, 2810, 4186, 4243, 2771, 3205]
  for index in Florida:
    df.loc[df.index == index, 'Stateprovince'] = 'Florida'

  Texas = [733, 1270, 2039, 2055, 1308, 3272, 4085]
  for index in Texas:
    df.loc[df.index == index, 'Stateprovince'] = 'Texas'

  df.loc[((df['City'] == 'Los Angeles') | (df['City'] == 'San Francisco') |
          (df['City'] == 'San Diego')), 'Stateprovince'] = 'California'
  California = [2357, 2360, 3184, 3244, 3613, 3673, 3674, 3958, 3978, 4419, 4420,
                4678, 5896, 7059, 7066, 6892, 212, 631, 2726, 2563, 2549, 2198,
                3247, 3253, 3372, 3971, 3988, 3999, 4189, 4326, 4516, 4517, 4659,
                4665, 1995, 155, 189, 251, 3982, 4999, 1710]
  for index in California:
    df.loc[df.index == index, 'Stateprovince'] = 'California'

  NY = [2883, 895, 912, 2936, 3002, 3089, 4183, 1968, 2000, 2769]
  for index in NY:
    df.loc[df.index == index, 'Stateprovince'] = 'New York'

  #Country
  df.loc[df.index == 730, 'Country'] = 'Serbia'
  df.loc[df.index == 138, 'Country'] = 'Georgia'
  df.loc[df.index == 2127, 'Country'] = 'South Africa'
  df.loc[df.index == 1582, 'Country'] = 'Denmark'
  df.loc[df.index == 4182, 'Country'] = 'Sweden'
  df.loc[((df.index == 3526) | (df.index == 4265) | (df.index == 4266)), 'Country'] = 'Netherlands'
  df.loc[df.index == 2730, 'Country'] = 'Russia'
  df.loc[df.index == 4258, 'Country'] = 'Iran'
  df.loc[df.index == 4260, 'Country'] = 'Israel'
  df.loc[df.index == 2166, 'Country'] = 'Uganda'
  df.loc[(df.index == 1669) | (df.index == 2523) | (df.index == 1666) | (df.index == 1668), 'Country'] = 'Ireland'
  df.loc[(df.index == 7067), 'Country'] = 'Vietnam'
  df.loc[(df.index == 4092), 'Country'] = 'Singapore'
  df.loc[(df.index == 2735), 'Country'] = 'New Zealand'
  df.loc[(df.index == 909), 'Country'] = 'Argentina'
  df.loc[(df.index == 2584), 'Country'] = 'Colombia'
  df.loc[(df.index == 13), 'Country'] = 'Tunisia'
  df.loc[((df.index == 3433) | (df.index == 2746) | (df.index == 2737)), 'Country'] = 'India'
  df.loc[((df.index == 1910) | (df.index == 2208)), 'Country'] = 'Switzerland'

  USA = [733, 827, 839, 895, 912, 1400, 1403, 1269, 1270, 1483, 1561, 1571, 1653,
         2187, 2243, 2357, 2360, 2599, 2631, 2632, 2803, 2810, 2883, 2936, 2987,
         3002, 3089, 3184, 3244, 3430, 3479, 3571, 3613, 3673, 3674, 3958, 3978,
         4034, 4108, 4183, 4186, 4243, 4334, 4419, 4420, 4506, 4571, 4605, 4668,
         4678, 4716, 5896, 6892, 7059, 7066, 212, 278, 333, 349, 355, 631, 699,
         702, 924, 1308, 1328, 1344, 1453, 1495, 1529, 1740, 1802, 1860, 2793,
         2771, 2726, 2563, 2549, 2198, 3247, 3253, 3372, 3971, 3988, 3999, 4189,
         4326, 4516, 4517, 4659, 4665, 2743, 4717, 2709, 2604, 2557, 2263, 1948,
         3106, 1922, 1922, 1980, 4645, 1995, 2999, 3130, 3170, 3205, 3272, 3352,
         3353, 3432, 3434, 3447, 4007, 4111, 4185, 4136, 4187, 155, 189, 251, 319,
         412, 1430, 3982, 4417, 4999, 5199, 1986, 1710, 2769]
  for index in USA:
    df.loc[df.index == index, 'Country'] = 'USA'

  CANADA = [1289, 1694, 1891, 2721, 2784, 3223, 3437, 4152, 4286, 4291, 4298,
            4310, 4315, 4321, 4449, 4497, 2748, 2749, 3344, 4202, 4453, 4456,
            4457, 4458, 4459, 4460, 4461, 4462, 4463, 4464, 4465, 4466, 4467,
            4468, 4469, 4470, 4471, 4472, 4473, 4477, 4498, 4499, 4500, 2652,
            1956, 2166, 3219, 3225, 3224, 3230, 3531, 4269, 4271, 4314, 4316,
            4318, 4319, 4320, 4647, 4322, 4323, 4324, 4325, 3173, 3174, 3175,
            3221, 3222, 4337, 4609, 4655, 2138, 4450]
  for index in CANADA:
    df.loc[df.index == index, 'Country'] = 'Canada'

  Australia = [929, 1920, 1925, 3278, 3280, 3378, 4199, 4212, 4213, 4214, 4215,
               4216, 4217, 4218, 4219, 4220, 4221, 4222, 4224, 4225, 4226, 4496,
               4211]
  for index in Australia:
    df.loc[df.index == index, 'Country'] = 'Australia'

  df.loc[df['Stateprovince'] == 'England', 'Country'] = 'United Kingdom'
  UK = [1705, 2332, 2333, 3524, 3532, 4413, 4664, 708, 1348, 2326, 2327, 2328,
        2329, 2330, 2331, 4117, 4118, 4119, 4400, 4612, 2254, 4009, 4327, 4648,
        2100, 2316, 5139]
  for index in UK:
    df.loc[df.index == index, 'Country'] = 'United Kingdom'

  df.loc[df['Country'] == 'DE', 'Country'] = 'Germany'
  Germany = [1839, 1916, 1917, 1965, 1971 ]
  for index in Germany:
    df.loc[df.index == index, 'Country'] = 'Germany'

  France = [2705, 3259, 3262, 3473, 4328, 3260, 3261, 68, 3257]
  for index in France:
    df.loc[df.index == index, 'Country'] = 'France'

  Italy = [3521, 3522, 3523]
  for index in Italy:
    df.loc[df.index == index, 'Country'] = 'Italy'

  Chile = [3991, 3992, 3993]
  for index in Chile:
    df.loc[df.index == index, 'Country'] = 'Chile'

  #Zip
  df.loc[df.index == 3247, 'ZIP'] = '90272'
  df.loc[df.index == 3372, 'ZIP'] = '91767'
  df.loc[df.index == 4189, 'ZIP'] = '94585'
  df.loc[df.index == 4516, 'ZIP'] = '92801'
  df.loc[df.index == 2743, 'ZIP'] = '49442'
  df.loc[df.index == 4612, 'ZIP'] = 'WN80QP'
  df.loc[df.index == 4085, 'ZIP'] = '75090'
  df.loc[df.index == 4111, 'ZIP'] = '08882'
  df.loc[df.index == 4187, 'ZIP'] = '54235'
  df.loc[df.index == 4599, 'ZIP'] = '54981'
  df.loc[df.index == 4999, 'ZIP'] = '92025'

  city_state_country_clean()

  #Fill in Full Address as much as possible using city, state, country
  mask = (df['Full Address'].isnull() &
        ~df['City'].isnull() &
        ~df['Stateprovince'].isnull() &
        ~df['Country'].isnull())
  df.loc[mask,'Full Address'] = df['City'] + ', ' + df['Stateprovince'] + ', ' + df['Country']

  df[['City','Stateprovince','Country','ZIP','Full Address']] = df[['City','Stateprovince','Country','ZIP','Full Address']].fillna('UNKNOWN')

#clean location data
location_clean()

#Donations
df.loc[((df['Donations'] == 'Toronto') |
        (df['Donations'] == 'Maybe')), 'Donations'] = 0
df['Donations'] = df['Donations'].str.strip('$')
df['Donations'] = df['Donations'].astype(float)

#Birthday
df['Birthday'] == 'UNKNOWN'

#Emails sent
df['Emails sent'].value_counts(dropna=False)
df.loc[((df['Emails sent'] == 'Toronto') | (df['Emails sent'].isnull())), 'Emails sent'] = 0
df['Emails sent'] = df['Emails sent'].astype(int)

#One Hot Interest
df = df.rename(columns = {'I am interested in':'interest'})
df.loc[df['interest'] == 'Toronto','interest'] = np.NaN
df['interest'] = df['interest'].fillna('')
df['interest_volunteer'] = df['interest'].str.lower().str.contains('volunteer')
df['interest_donate'] = df['interest'].str.lower().str.contains('donat')
df['interest_partner'] = df['interest'].str.lower().str.contains('partner')
df['interest_join_mm'] = df['interest'].str.lower().str.contains('join')
df['interest_media'] = df['interest'].str.lower().str.contains('media')
df['interest_other'] = df['interest'].str.lower().str.contains('else')
df.loc[(~df['interest_volunteer'] & ~df['interest_donate'] &
        ~df['interest_partner'] & ~df['interest_join_mm'] &
        ~df['interest_media']),'interest_other'] = True

#Name
df.loc[df['Name'] == 'Toronto', 'Name'] = np.NaN

#Merge Status
df.loc[df['Merge status'] == 'Toronto', 'Merge status'] = np.NaN

#Clicked
df.loc[((df['Clicked'] == 'Toronto') | (df['Clicked'].isnull())), 'Clicked'] = 0
df['Clicked'] = df['Clicked'].astype(int)

#Opened
df.loc[((df['Opened'] == 'Toronto') | (df['Opened'].isnull())), 'Opened'] = 0
df['Opened'] = df['Opened'].astype(int)

#Source
df.loc[df['Source'] == 'Toronto', 'Source'] = np.NaN

#Donation Date
df.loc[df['Donation Date'] == 'Toronto', 'Donation Date'] = np.NaN
df['Donation Date'] = pd.to_datetime(df['Donation Date'])


matt_clean()

df = df.drop(columns = ['Timezone', 'Donations', 'Donation Date',
                        'Donation Source', 'Source', 'Company', 'Emails sent',
                        'Opened', 'Clicked', 'Birthday', 'Name',
                        'Merge status'])

schema = """CREATE TABLE Miracle_Messages(
                    Submitted On TIMESTAMP WITHOUT TIMEZONE,
                    Email VARCHAR(70) nullable==False,
                    First Name VARCHAR(25) nullable==False,
                    Last Name VARCHAR(25) nullable==False,
                    Phone BIGINTEGER,
                    City VARCHAR(85) nullable==False,
                    State/Province VARCHAR(85) nullable==False,
                    Interested In VARCHAR(150) nullable==False,
                    Interest Volunteer BOOLEAN nullable==False,
                    Interest Donate BOOLEAN nullable==False,
                    Interest Partner BOOLEAN nullable==False,
                    Interest Join_mm BOOLEAN nullable==False,
                    Interest Media BOOLEAN nullable==False,
                    Interest Other BOOLEAN nullable==False,
                    Comment VARCHAR(2500),
                    Postal Code VARCHAR(10) nullable==False,
                    Full Address VARCHAR(300),
                    PRIMARY KEY(Email)
                    );"""

df.to_sql(name = 'miracle_test_db',
          con = conn,
          schema = schema,
          if_exists='replace',
          index=False)

df.to_json(path_or_buf="""/Users/mattkirby/Desktop/miracle_messages.json""",
           orient='columns')

df.to_csv(path_or_buf="""/Users/mattkirby/Desktop/miracle_messages.csv""")
