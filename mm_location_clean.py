import pandas as pd
import numpy as np
from re import search
import string

csv = '/Users/mattkirby/Desktop/Master MM Volunteers - MM Volunteers.csv'
df = pd.read_csv(csv)

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

aus_states = {'NSW':'New South Wales',
              'QLD':'Queensland',
              'SA':'South Australia',
              'TAS':'Tasmania',
              'VIC':'Victoria',
              'WA':'Western Australia'}

US_cities = ['Boston', 'Los Angeles', 'San Diego', 'San Francisco',
             'Salt Lake City', 'Chicago', 'New York City', 'Dallas',
             'Cincinnati', 'Baltimore', 'Seattle', 'Philadelphia',
             'New Orleans', 'Nashville', 'Pittsburgh', 'Memphis',
             'Charlottesville', 'Knoxville', 'Miami', 'Orlando', 'Tampa']

city_state = {'San Diego':'California', 'San Francisco':'California',
              'Los Angeles':'California', 'Boston':'Massachusetts',
              'Salt Lake City':'Utah', 'Chicago':'Illinois',
              'New York City':'New York', 'Dallas':'Texas', 'Houston':'Texas',
              'Fort Worth':'Texas', 'Cincinnati':'Ohio', 'Baltimore':'Maryland',
              'Seattle':'Washington', 'Philadelphia':'Pennsylvania',
              'Pittsburgh':'Pennsylvania', 'New Orleans':'Louisiana',
              'Nashville':'Tennessee', 'Memphis':'Tennessee',
              'Charlottesville':'Virginia', 'Knoxville':'Tennessee',
              'Miami':'Florida', 'Orlando':'Florida', 'Tampa':'Florida',
              'London':'England', 'Paris':'Paris', 'Toronto':'Ontario',
              'Barcelona':'Barcelona', 'Brighton':'England',
              'Manchester':'England', 'Montreal':'Quebec',
              'Brisbane':'Queensland', 'Melbourne':'Victoria',
              'Perth':'Western Australia'}

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

def common_city_state():
  for city, state in city_state.items():
    df.loc[(~df['City'].isnull()) &
           ((df['City'].str.lower().str.contains(state.lower())) |
           (df['City'].str.lower().str.contains(city.lower()))),
           'Stateprovince'] = state
    df.loc[(~df['City'].isnull()) &
           (df['City'].str.lower().str.contains(city.lower())), 'City'] = city

    df.loc[(~df['Stateprovince'].isnull()) &
           (df['Stateprovince'].str.lower().str.contains(city.lower())),
           'City'] = city
    df.loc[(~df['Stateprovince'].isnull()) &
           (df['Stateprovince'].str.lower().str.contains(state.lower())),
           'Stateprovince'] = state

    df.loc[df['City'].isnull() &
           df['Full Address'].str.lower().str.contains(city.lower()),
           'City'] = city
    df.loc[df['Stateprovince'].isnull() &
           df['Full Address'].str.lower().str.contains(state.lower()),
           'Stateprovince'] = state

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

def strip_punc():
  for col in ['City','Stateprovince','Country']:
    df[col] = df[col].apply(lambda s: str(s).translate(
                      str.maketrans('', '', string.punctuation)))

  #converted nan to string, need to convert back
  df.loc[df['Country'] == 'nan','Country'] = np.NaN
  df.loc[df['Stateprovince'] == 'nan','Stateprovince'] = np.NaN
  df.loc[df['City'] == 'nan','City'] = np.NaN

def us_states():
  for abr in states:
    df.loc[((df['Full Address'].str.contains(states[abr]) &
             df['Stateprovince'].isnull())),
            'Stateprovince'] = states[abr]
    df.loc[((df['Full Address'].str.lower().str.contains(' '+abr.lower()+' ') &
             df['Stateprovince'].isnull())),
            'Stateprovince'] = states[abr]
    df.loc[(df['Country'] == 'United States') & (df['Stateprovince'].isnull()) &
           (df['Full Address'].str.contains(abr)),
           'Stateprovince'] = states[abr]
    df.loc[(df['Stateprovince'].isnull()) &
           (df['City'].str.lower().str.endswith(' '+ abr.lower()) |
            df['Full Address'].str.lower().str.endswith(' '+abr.lower())),
           'Stateprovince'] = states[abr]
    df.loc[df['Stateprovince'].str.lower() == states[abr].lower(),
           'Country'] = 'United States'
    df.loc[df['Stateprovince'].str.lower() == abr.lower(),
           'Stateprovince'] = states[abr]
    df.loc[df['Country'].isnull() &
           ((df['Stateprovince'].str.lower() == abr.lower()) |
            (df['Stateprovince'].str.lower() == states[abr].lower())),
           'Country'] = 'United States'
    df.loc[(~df['Stateprovince'].isnull() &
            (df['Stateprovince'].str.lower().str.contains(states[abr].lower()) |
            (df['Stateprovince'].str.lower().str.startswith(abr.lower()+' ')))),
            'Stateprovince'] = states[abr]

def can_states():
    for abr in can_provinces:
      df.loc[((df['Full Address'].str.contains(can_provinces[abr]) &
               df['Stateprovince'].isnull())),
              'Stateprovince'] = can_provinces[abr]
      df.loc[((df['Full Address'].str.lower().str.contains(' '+abr.lower()+' ')&
               df['Stateprovince'].isnull())),
              'Stateprovince'] = can_provinces[abr]
      df.loc[(df['Country'] == 'Canada') & (df['Stateprovince'].isnull()) &
             (df['Full Address'].str.contains(abr)),
             'Stateprovince'] = can_provinces[abr]
      df.loc[(df['Stateprovince'].isnull()) &
             (df['City'].str.lower().str.endswith(' '+ abr.lower()) |
              df['Full Address'].str.lower().str.endswith(' '+abr.lower())),
             'Stateprovince'] = can_provinces[abr]
      df.loc[df['Stateprovince'].str.lower() == can_provinces[abr].lower(),
             'Country'] = 'Canada'
      df.loc[df['Stateprovince'].str.lower() == abr.lower(),
             'Stateprovince'] = can_provinces[abr]
      df.loc[df['Country'].isnull() &
             ((df['Stateprovince'].str.lower() == abr.lower()) |
              (df['Stateprovince'].str.lower() == can_provinces[abr].lower())),
             'Country'] = 'Canada'
      df.loc[(~df['Stateprovince'].isnull() &
            (df['Stateprovince'].str.lower().str.contains(can_provinces[abr].lower()) |
            (df['Stateprovince'].str.lower().str.startswith(abr.lower()+' ')))),
            'Stateprovince'] = can_provinces[abr]

def australia_states():
  for abr in aus_states:
    df.loc[((df['Full Address'].str.contains(aus_states[abr]) &
             df['Stateprovince'].isnull())),
            'Stateprovince'] = aus_states[abr]
    df.loc[((df['Full Address'].str.lower().str.contains(' '+abr.lower()+' ') &
             df['Stateprovince'].isnull())),
            'Stateprovince'] = aus_states[abr]
    df.loc[(df['Country'] == 'Australia') & (df['Stateprovince'].isnull()) &
           (df['Full Address'].str.contains(abr)),
           'Stateprovince'] = aus_states[abr]
    df.loc[(df['Stateprovince'].isnull()) &
           (df['City'].str.lower().str.endswith(' '+ abr.lower()) |
            df['Full Address'].str.lower().str.endswith(' '+abr.lower())),
           'Stateprovince'] = aus_states[abr]
    df.loc[df['Stateprovince'].str.lower() == aus_states[abr].lower(),
           'Country'] = 'Australia'
    df.loc[df['Stateprovince'].str.lower() == abr.lower(),
           'Stateprovince'] = aus_states[abr]
    df.loc[df['Country'].isnull() &
           ((df['Stateprovince'].str.lower() == abr.lower()) |
            (df['Stateprovince'].str.lower() == aus_states[abr].lower())),
           'Country'] = 'Australia'
    df.loc[(~df['Stateprovince'].isnull() &
            (df['Stateprovince'].str.lower().str.contains(aus_states[abr].lower()) |
            (df['Stateprovince'].str.lower().str.startswith(abr.lower()+' ')))),
            'Stateprovince'] = aus_states[abr]

def common_countries():
  for abr in countries:
    df.loc[df['Country'].str.lower() == abr.lower(),
           'Country'] = countries[abr]
    df.loc[((df['City'].str.lower() == abr.lower()) |
           (df['City'].str.lower() == countries[abr].lower())) &
           df['Country'].isnull(), 'Country'] = countries[abr]
    df.loc[((df['Stateprovince'].str.lower() == abr.lower()) |
           (df['Stateprovince'].str.lower() == countries[abr].lower())) &
           df['Country'].isnull(), 'Country'] = countries[abr]
    df.loc[df['Country'].isnull() & ((df['Full Address'].str.lower() == abr.lower()) |
           df['Full Address'].str.lower().str.contains(countries[abr].lower())),
           'Country'] = countries[abr]

def postal_codes():
  #US Postal Codes from Full Address
  for i in df['Full Address'].index:
    if ((df[df.index == i]['ZIP'].apply(type).any() != str) &
        (df[df.index == i]['Full Address'].apply(type).any() == str)):
      if search('\d{5}', df.loc[i, 'Full Address']):
        df.loc[i, 'ZIP'] = search('\d{5}', df.loc[i, 'Full Address']).group(0)

  #One off zips found in other fields
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

def break_cities():
  #Get City, and if statprovince is empty,
  #dump everything after first comma there
  temp = df['City'].str.split(',')
  df['City'] = temp.str[0]
  mask = df['Stateprovince'].isnull()
  df.loc[mask, 'Stateprovince'] = temp.loc[mask].str[1:]
  df.loc[mask, 'Stateprovince'] = [try_join(l) for l in
                                   df.loc[mask, 'Stateprovince']]

def fill_full_address():
  #Fill in Full Address as much as possible using city, state, country
  mask = (df['Full Address'].isnull() &
        ~df['City'].isnull() &
        ~df['Stateprovince'].isnull() &
        ~df['Country'].isnull())
  df.loc[mask,'Full Address'] = (df['City'] + ', ' + df['Stateprovince'] +
                                 ', ' + df['Country'])

def one_off_cities():
  id_and_city = {711:'Bath', 4406:'Sydney', 2276:'Liverpool', 2949:'New Castle',
                 2682:'Minneapolis', 730:'Belgrade', 2793:'Bridgeport',
                 733:'Belton', 912:'Buffalo', 929:'Byron Bay', 1483:'Columbus',
                 1571:'Daytona Beach', 1585:'Denton', 1653:'Detroit',
                 1735:'Fallbrook', 2039:'Houston', 2187:'Monroe',
                 2243:'Las Vegas', 2721:'Montreal', 2784:'Nelson',
                 2803:'New Orleans',2810:'Orlando', 2987:'Philadelphia',
                 3015:'Fremont', 3184:'Orange County', 3451:'Raleigh',
                 3571:'Salt Lake City', 3958:'Santa Cruz', 3978:'Santa Rosa',
                 4034:'Seattle', 4108:'Lake Tahoe', 4182:'Stockholm',
                 4183:'Stony Brook', 4186:'Stuart', 4243:'Tampa',
                 4334:'Spokane', 4413:'Brighton', 4497:'Victoria',
                 4571:'Williamsport', 4668:'York', 4678:'Stanford',
                 4716:'Arlington', 5896:'Burlingame', 6892:'Oakland',
                 212:'Monrovia', 333:'Pittsburgh', 349:'Memphis', 355:'Toledo',
                 539:'Alexandria', 631:'Nevada City', 699:'Baltimore',
                 702:'Bangor',837:'Boulder City', 852:'Brasilia', 903:'Bryan',
                 909:'Buenos Aires', 924:'Burlington', 1283:'Cambridge',
                 1293:'Canton', 1308:'Carrollton', 1328:'Knoxville',
                 1344:'Charlottesville', 1348:'Chelmsford', 1453:'Collierville',
                 1839:'Frankfurt', 1916:'Aachen', 4665:'Novato',
                 3531:'Wallaceburg', 2523:'Cork', 2100:'Ipswich',
                 4612:'Roby Hill', 1980:'New London', 1995:'Hesperia',
                 1995:'Kampala', 2730:'Moscow', 1917:'Saarbrucken',
                 3170:'Huntington', 3205:'Huntington', 4092:'Singapore',
                 2127:'Johannesburg', 319:'New Mexico', 2138:'Kamloomps',
                 3982:'Santa Rosa', 4999:'Escondido', 13:'Sousse',
                 1666:'Dublin', 1668:'Dublin', 3613:'San Diego',
                 4517:'San Diego', 4506:'Virginia Beach', 2983:'Virginia Beach',
                 4548:'Warrington', 4571:'Washington', 3434:'Washington',
                 3130:'Cleveland', 1430:'Cleveland', 4842:'Paris',
                 1321:'Paris', 1469:'Columbia', 1470:'Columbia', 839:'Boulder',
                 840:'Boulder', 1289:'Vancouver', 4449:'Vancouver',
                 3223:'Ottawa', 3437:'Ottawa', 2631:'Miami', 2632:'Miami',
                 2333:'London', 708:'London', 4327:'London', 2285:'London',
                 2316:'London', 2332:'London', 2333:'London', 2007:np.NaN,
                 2008:np.NaN, 869:np.NaN, 1709:np.NaN, 1710:np.NaN,
                 278:'Nashville', 2758:'Nashville', 2759:'Nashville',
                 2760:'Nashville', 412:'Portland', 2357:'Los Angeles',
                 2360:'Los Angeles', 189:'Los Angeles', 251:'Los Angeles',
                 1400:'Chicago', 1403:'Chicago', 2263:'Chicago', 1948:'Chicago',
                 3106:'Chicago', 4185:'Chicago', 2957:'Boston', 827:'Boston',
                 1495:'Boston', 1270:'Dallas', 2055:'Dallas', 72:'Dallas',
                 4286:'Toronto', 4310:'Toronto', 4315:'Toronto', 4321:'Toronto',
                 3244:'San Francisco', 3673:'San Francisco',
                 3674:'San Francisco', 4419:'San Francisco',
                 4420:'San Francisco', 7059:'San Francisco',
                 7066:'San Francisco', 155:'San Francisco',
                 2883:'New York City', 895:'New York City',
                 2936:'New York City', 3002:'New York City',
                 3089:'New York City', 891:'New York City',
                 896:'New York City', 1968:'New York City',
                 2769:'New York City'}


  df.loc[df['City'].isnull() &
       df['Full Address'].str.contains('Toronto')] = 'Toronto'
  mask = (df['Stateprovince'].isnull() &
          df['Full Address'].str.contains('Sydney'))
  df.loc[mask, 'City'] = 'Sydney'
  df.loc[mask, 'Stateprovince'] = 'New South Wales'
  df.loc[mask, 'Country'] = 'Australia'

  df.loc[(~df['City'].isnull() | df['City'].isnull()) &
         df['Full Address'].str.contains('London'), 'City'] = 'London'

  for i, city in id_and_city.items():
    df.loc[(df.index == i), 'City'] = city

def one_off_countries():
  id_and_country = {
      730:'Serbia', 138:'Georgia', 2127:'South Africa', 1582:'Denmark',
      4182:'Sweden', 2730:'Russia', 4258:'Iran', 4260:'Israel', 2166:'Uganda',
      7067:'Vietnam', 4092:'Singapore', 2735:'New Zealand', 909:'Argentina',
      2584:'Colombia', 13:'Tunisia', 1669:'Ireland', 2523:'Ireland',
      1666:'Ireland', 1668:'Ireland', 3526:'Netherlands', 4265:'Netherlands',
      4266:'Netherlands', 3433:'India', 2746:'India', 2737:'India',
      1910:'Switzerland', 2208:'Switzerland', 3521:'Italy', 3522:'Italy',
      3523:'Italy', 3991:'Chile', 3992:'Chile', 3993:'Chile'}

  df.loc[df['Stateprovince'] == 'England', 'Country'] = 'United Kingdom'

  for i, country in id_and_country.items():
    df.loc[(df.index == i), 'Country'] = country

def one_off_states():
  id_and_state = {
      1986:'Puerto Rico', 68:'Gard', 5389:'Vienne', 5850:'Vence',
      674:'Provence', 2505:'Auvergne', 4828:'Bordeaux', 4905:'Manche',
      2682:'Minnesota', 1917:'Saarland', 730:'Belgrade', 4337:'Nova Scotia',
      4609:'Alberta', 4655:'Manitoba', 319:'New Mexico', 2187:'New Hampshire',
      3170:'West Virginia', 4182:'Uppland', 4413:'Sussex', 2100:'Suffolk',
      1348:'Essex', 4571:'District of Columbia', 702:'Maine',
      852:'Central West', 909:'Buenos Aires', 1802:'Georgia', 1839:'Hesse',
      1860:'South Carolina', 1910:'Geneva', 2523:'Munster', 4092:'Singapore',
      2730:'Central', 2127:'Guateng', 476:'Scotland', 3:'Indiana',
      1965:'Hamburg', 1971:'Lower Saxony', 138:'Tbilisi', 3447:'North Carolina',
      2166:'Wakiso District', 1916:'North Rhine-Westphalia', 2208:'Zurich',
      39:'Canberra', 13:'Sousse', 5464:'Occitanie', 5834:'Occitanie',
      4846:'Cher', 4847:'Cher', 2557:'New Jersey', 4111:'New Jersey',
      5199:'New Jersey', 839:'Colorado', 3432:'Colorado', 4404:'Victoria',
      327:'Victoria', 1653:'Michigan', 2743:'Michigan', 2721:'Quebec',
      2722:'Quebec', 924:'Vermont', 2999:'Vermont', 1666:'Leinster',
      1668:'Leinster', 4007:'Wisconsin', 4187:'Wisconsin', 4599:'Wisconsin',
      1483:'Ohio', 355:'Ohio', 3130:'Ohio', 4034:'Washington',
      4334:'Washington', 4014:'Washington', 2187:'Louisiana', 2803:'Louisiana',
      3433:'Maharashtra', 2746:'Maharashtra'}

  for i, state in id_and_state.items():
    df.loc[(df.index == i), 'State'] = state

def capitalize():
  df['City'] = df['City'].str.title()
  df['Stateprovince'] = df['Stateprovince'].str.title()
  df['Country'] = df['Country'].str.title()

def fill_null():
  df[['City','Stateprovince','Country',
      'ZIP','Full Address']] = df[['City','Stateprovince','Country',
                                   'ZIP','Full Address']].fillna('UNKNOWN')

def clean_mm_location_cols():
    strip_nan()
    break_cities()
    strip_punc()
    postal_codes()
    one_off_cities()
    common_city_state()
    one_off_states()
    one_off_countries
    strip_nan()
    common_countries()
    us_states()
    can_states()
    australia_states()
    capitalize()
    fill_full_address()
    fill_null()
    return df
