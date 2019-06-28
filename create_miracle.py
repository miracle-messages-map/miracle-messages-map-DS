import sqlite3
import pandas as pd
import numpy as np
from re import search
from mm_location_clean import clean_mm_location_cols
from validate_email import validate_email


conn = sqlite3.connect("""/Users/mattkirby/Desktop/miracle_messages.sqlite3""")
curs = conn.cursor()

df = clean_mm_location_cols()

df['Valid Email?'] = df['Email address'].apply(lambda x:validate_email(x))

def Stan_clean():
    df.loc[df['City'] == 'Spring Hill']

    df.iloc[5777:][:4]['Timezone'] = 'America/Indiana/Knox'

    df.iloc[5777:][:4]

    df.iloc[4720:][:1]['Timezone'] = 'America/Los_Angeles'

    df.iloc[4722:][:1]['Timezone'] = 'America/Los_Angeles'

    df.iloc[5827:][:1]['Timezone'] = 'America/Los_Angeles'

    df.iloc[36:][:1]['Timezone'] = 'America/Los_Angeles'

    df.iloc[5508:][:2]['Timezone'] = 'America/Los_Angeles'

    df.iloc[4982:][:2]['Timezone'] = 'America/New_York'

    df.iloc[4812:][:2]['Timezone'] = 'America/Los_Angeles'

    df.iloc[5521:][:2]['Timezone'] = 'America/Los_Angeles'

    df.loc[df['Email address'] == 'rsarabi@gmail.com']

    df.iloc[34:][:1]['Timezone'] = 'America/Los_Angeles'

    df.iloc[5506:][:2]['Timezone'] = 'America/Los_Angeles'

    df.loc[df['Email address'] == 'mark.french@usa.net']

    df.iloc[792:][:1]['Timezone'] = 'America/New_York'

    df.iloc[5131:][:1]['Timezone'] = 'America/New_York'

    df.loc[df['Email address'] == 'jenasysart@gmail.com']

    df.iloc[4777:][:1]['Timezone'] = 'America/Los_Angeles'

    df.loc[df['Email address'] == 'higa@stanford.edu']

    df.iloc[5791:][:2]['Timezone'] = 'America/Los_Angeles'

    df.loc[df['Email address'] == 'quawish@hotmail.com']

    df.iloc[5785:][:2]['Timezone'] = 'America/Toronto'

    df.loc[df['Email address'] == 'abigailde217@gmail.com']

    df.iloc[5490:][:1]['Timezone'] = 'America/Los_Angeles'

    df.loc[df['Email address'] == 'maschulen@gmail.com']

    df.iloc[4753:][:1]['Timezone'] = 'America/North_Dakota/New_Salem'

    df.loc[df['Email address'] == 'jonescat00@gmail.com']

    df.iloc[5449:][:2]['Timezone'] = 'America/Los_Angeles'

    df.loc[df['Email address'] == 'test@test.com']

    df.loc[df['Email address'] == 'hello@miraclemessages.org']

    df.iloc[4719:][:1]['Timezone'] = 'America/Los_Angeles'

    df.loc[df['Email address'] == 'jhaddad0324@gmail.com']

    df.iloc[4978:][:2]['Timezone'] = 'America/New_York'

    df.loc[df['Email address'] == 'napier.lisa@gmail.com']

    df.iloc[4276:][:1]['Timezone'] = 'America/Indiana/Indianapolis'

    df.iloc[5830:][:1]['Timezone'] = 'America/Indiana/Indianapolis'

    df.loc[df['Email address'] == 'casacuriel@gmail.com']

    df.iloc[5172:][:2]['Timezone'] = 'America/Los_Angeles'

    df.loc[df['Email address'] == 'esassenrath@gmail.com']

    df.iloc[5168:][:1]['Timezone'] = 'America/Los_Angeles'

    df.loc[df['Email address'] == 'g_oh1@u.pacific.edu']

    df.iloc[4810:][:2]['Timezone'] = 'America/Los_Angeles'

    df.loc[df['Email address'] == 'g_oh1@u.pacific.edu']

    df.loc[df['Email address'] == 'pscroce@gmail.com']

    df.iloc[5271:][:1]['Timezone'] = 'America/New_York'

    df.iloc[6438:][:1]['Timezone'] = 'America/New_York'

    df.loc[df['Email address'] == 'smyers969@hotmail.com']

    df.iloc[5105:][:1]['Timezone'] = 'America/Los_Angeles'

    df.loc[df['Email address'] == 'ianamagruder@gmail.com']

    df.iloc[465:][:1]['Timezone'] = 'America/Los_Angeles'

    df.iloc[5610:][:1]['Timezone'] = 'America/Los_Angeles'

    df.loc[df['Email address'] == 'Karen.sinche413@gmail.com']

    df.iloc[4995:][:1]['Timezone'] = 'America/New_York'

    df.iloc[7096:][:1]['Timezone'] = 'America/New_York'

    df.loc[df['Email address'] == 'cookiesss@aol.com']

    df.iloc[5733:][:1]['Timezone'] = 'America/Los_Angeles'

    df.loc[df['Email address'] == 'nae32123@yahoo.com']

    df.iloc[5353:][:2]['Timezone'] = 'America/New_York'

    df.loc[df['Email address'] == 'crgutman@gmail.com']

    df.iloc[5676:][:1]['Timezone'] = 'America/Los_Angeles'

    df.loc[df['Email address'] == 'kimmiepeacheskp.68@gmail.com']

    df.iloc[4788:][:2]['Timezone'] = 'America/New_York'

    df.loc[df['Email address'] == 'pranped@yahoo.com']

    df.iloc[5713:][:1]['Timezone'] = 'America/Los_Angeles'

    df.iloc[5715:][:1]['Timezone'] = 'America/Los_Angeles'

    df.loc[df['Email address'] == 'arisabsovich@yahoo.com']

    df.iloc[5487:][:1]['Timezone'] = 'America/Los_Angeles'

    df.iloc[5896:][:1]['Timezone'] = 'America/Los_Angeles'

    df.loc[df['Email address'] == 'win.raguini@gmail.com']

    df.iloc[5650:][:1]['Timezone'] = 'America/Los_Angeles'

    df.loc[df['Email address'] == 'letang.lewis@gmail.com']

    df.iloc[5638:][:1]['Timezone'] = 'America/Los_Angeles'

    df.iloc[5671:][:1]['Timezone'] = 'America/Los_Angeles'

    df.loc[df['Email address'] == 'rndarcy@yahoo.com']

    df.iloc[5427:][:1]['Timezone'] = 'America/Los_Angeles'

    df.loc[df['Email address'] == 'sfowler@businesscentral.net']

    df.iloc[4892:][:2]['Timezone'] = 'America/Los_Angeles'

    df.loc[df['Email address'] == 'nvsanto@gmail.com']

    df.iloc[4690:][:1]['Timezone'] = 'America/Los_Angeles'

    df.loc[df['Email address'] == 'jessdonig@gmail.com']

    df.iloc[4723:][:1]['Timezone'] = 'America/Los_Angeles'

    df.loc[df['Email address'] == 'merchan1313@gmail.com']

    df.iloc[5222:][:1]['Timezone'] = 'America/Nassau'

    df.loc[df['Email address'] == 'jessdonig@gmail.com']

    df.iloc[4723:][:1]['Timezone'] = 'America/Los_Angeles'

    df.loc[df['Email address'] == 'pinecabin@yahoo.com']

    df.iloc[4675:][:1]['Timezone'] = 'America/Indiana/Knox'

    df.iloc[5240:][:1]['Timezone'] = 'America/Indiana/Knox'

    df.loc[df['Email address'] == 'ligiamelo@stanford.edu']

    df.iloc[5796:][:2]['Timezone'] = 'America/Los_Angeles'

    df.loc[df['Email address'] == 'b.minott@me.com']

    df.iloc[5617:][:1]['Timezone'] = 'America/Los_Angeles'

    df.iloc[5640:][:1]['Timezone'] = 'America/Los_Angeles'

    df.loc[df['Email address'] == 'dguim2006@gmail.com']

    df.iloc[5735:][:1]['Timezone'] = 'America/Santiago'

    df.iloc[7044:][:1]['Timezone'] = 'America/Santiago'

    df.loc[df['Email address'] == 'Erica.Schwartzmann@asu.edu']

    df.iloc[4717:][:1]['Timezone'] = 'America/Phoenix'

    df.iloc[5373:][:1]['Timezone'] = 'America/Phoenix'

    df.loc[df['Email address'] == 'agentrayda@gmail.com']

    df.iloc[5817:][:1]['Timezone'] = 'America/New_York'

    df.loc[df['Email address'] == 'sagathe.finck@gmail.com']

    df.iloc[5243:][:1]['Timezone'] = 'America/Los_Angeles'

    df.loc[df['Email address'] == 'susanjnelson@capitolagirl.com']

    df.iloc[5851:][:2]['Timezone'] = 'America/Los_Angeles'

    df.loc[df['Email address'] == 'alexarzt@gmail.com']

    df.iloc[4708:][:1]['Timezone'] = 'America/Los_Angeles'

    df.iloc[5600:][:1]['Timezone'] = 'America/Los_Angeles'

    df.loc[df['Email address'] == 'kevinsrichmond@yahoo.com']

    df.iloc[1570:][:1]['Timezone'] = 'America/New_York'

    df.iloc[4960:][:1]['Timezone'] = 'America/New_York'

    df.loc[df['Email address'] == 'vhunleywhat@gmail.com']

    df.iloc[5011:][:1]['Timezone'] = 'America/Detroit'

    df.loc[df['Email address'] == 'b9kozmo@gmail.com']

    df.iloc[5195:][:2]['Timezone'] = 'America/Toronto'

    df.loc[df['Email address'] == 'giants6365@verizon.net']

    df.iloc[5128:][:2]['Timezone'] = 'America/New_York'

    df.loc[df['Email address'] == 'Swansonp32@gmail.com']

    df.iloc[5483:][:2]['Timezone'] = 'America/Los_Angeles'

    df.loc[df['Email address'] == 'glennraskin@gmail.com']

    df.iloc[5308:][:2]['Timezone'] = 'America/Los_Angeles'

    df.loc[df['Email address'] == 'sgn115@gmail.com']

    df.iloc[5262:][:1]['Timezone'] = 'America/New_York'

    df.loc[df['Email address'] == 'ben.woosley@gmail.com']

    df.iloc[5683:][:1]['Timezone'] = 'America/Los_Angeles'

    df.loc[df['Country'] == 'DK']

    df.iloc[1300:][:1]['Timezone'] = 'Europe/Copenhagen'

    df.loc[df['City'] == 'Abbeville']

    df.iloc[4734:][:1]['Timezone'] = 'America/Chicago'

    df.loc[(df['City'] == 'San Francisco'), ['Timezone']] = 'America/Los_Angeles'
    df.loc[(df['City'] == 'New York City'), ['Timezone']] = 'America/New_York'
    df.loc[(df['City'] == 'Los Angeles'), ['Timezone']] = 'America/Los_Angeles'
    df.loc[(df['City'] == 'San Diego'), ['Timezone']] ='America/Los_Angeles'
    df.loc[(df['Country'] == 'GB'), ['Timezone']] = 'Atlantic/Faroe'
    df.loc[(df['Country'] == 'England'), ['Timezone']] = 'Atlantic/Faroe'
    df.loc[(df['Country'] == 'United Kingdom'), ['Timezone']] = 'Atlantic/Faroe'
    df.loc[(df['City'] == 'Chicago'), ['Timezone']] = 'America/Chicago'
    df.loc[(df['City'] == 'Washington'), ['Timezone']] = 'America/New_York'
    df.loc[(df['City'] == 'Denver'), ['Timezone']] = 'America/Denver'
    df.loc[(df['City'] == 'Oakland'), ['Timezone']] = 'America/Los_Angeles'
    df.loc[(df['City'] == 'Boston'), ['Timezone']] = 'America/New_York'
    df.loc[(df['City'] == 'Austin'), ['Timezone']] = 'America/Chicago'
    df.loc[(df['City'] == 'Portland'), ['Timezone']] = 'America/Los_Angeles'
    df.loc[(df['City'] == 'Philadelphia'), ['Timezone']] = 'America/New_York'
    df.loc[(df['City'] == 'San Jose'), ['Timezone']] = 'America/Los_Angeles'
    df.loc[(df['City'] == 'Las Vegas'), ['Timezone']] = 'America/Los_Angeles'
    df.loc[(df['City'] == 'Berkley'), ['Timezone']] = 'America/Los_Angeles'
    df.loc[(df['City'] == 'Atlanta'), ['Timezone']] = 'America/Kentucky/Monticello'
    df.loc[(df['City'] == 'Pheonix'), ['Timezone']] = 'America/Pheonix'
    df.loc[(df['City'] == 'Brooklyn'), ['Timezone']] = 'America/New_York'
    df.loc[(df['City'] == 'Sacramento'), ['Timezone']] = 'America/Los_Angeles'
    df.loc[(df['City'] == 'Dallas'), ['Timezone']] = 'America/Indiana/Tell_City'
    df.loc[(df['City'] == 'Houston'), ['Timezone']] = 'America/Indiana/Tell_City'
    df.loc[(df['City'] == 'Salt Lake City'), ['Timezone']] = 'America/Shiprock'
    df.loc[(df['City'] == 'Santa Cruz'), ['Timezone']] = 'America/Los_Angeles'
    df.loc[(df['City'] == 'Miami'), ['Timezone']] = 'America/New_York'
    df.loc[(df['City'] == 'San Antonio'), ['Timezone']] = 'America/Monterrey'
    df.loc[(df['City'] == 'Baltimore'), ['Timezone']] = 'America/New_York'
    df.loc[(df['City'] == 'Long Beach'), ['Timezone']] = 'America/Los_Angeles'
    df.loc[(df['City'] == 'Charlotte'), ['Timezone']] = 'America/New_York'
    df.loc[(df['City'] == 'Concord'), ['Timezone']] = 'America/New_York'
    df.loc[(df['City'] == 'Santa Cruz'), ['Timezone']] = 'America/Los_Angeles'
    df.loc[(df['City'] == 'Columbus'), ['Timezone']] = 'America/Indiana/Vevay'
    df.loc[(df['City'] == 'Detroit'), ['Timezone']] = 'America/Detroit'
    df.loc[(df['City'] == 'Sydney, Australia'), ['Timezone']] = 'Australia/Sydney'
    df.loc[(df['City'] == 'Stanford'), ['Timezone']] = 'America/Los_Angeles'
    df.loc[(df['City'] == 'Fort Lauderdale'), ['Timezone']] = 'America/Nassau'
    df.loc[(df['City'] == 'Madison'), ['Timezone']] = 'America/Toronto'
    df.loc[(df['City'] == 'Arlington'), ['Timezone']] = 'America/New_York'
    df.loc[(df['City'] == 'Honolulu'), ['Timezone']] = 'Pacific/Honolulu'
    df.loc[(df['City'] == 'Grand Rapids'), ['Timezone']] = 'America/Toronto'

    df.loc[(df['Stateprovince'] == 'CA'), ['Timezone']] = 'America/Los_Angeles'
    df.loc[(df['Stateprovince'] == 'California'), ['Timezone']] = 'America/Los_Angeles'
    df.loc[(df['Stateprovince'] == 'NY'), ['Timezone']] = 'America/New_York'
    df.loc[(df['Stateprovince'] == 'MA'), ['Timezone']] = 'America/New_York'
    df.loc[(df['Stateprovince'] == 'CO'), ['Timezone']] = 'America/Denver'
    df.loc[(df['Stateprovince'] == 'FL'), ['Timezone']] = 'America/New_York'
    df.loc[(df['Stateprovince'] == 'WA'), ['Timezone']] = 'America/Los_Angeles'
    df.loc[(df['Stateprovince'] == 'IL'), ['Timezone']] = 'America/Chicago'
    df.loc[(df['Stateprovince'] == 'PA'), ['Timezone']] = 'America/New_York'
    df.loc[(df['Stateprovince'] == 'OR'), ['Timezone']] = 'America/Los_Angeles'
    df.loc[(df['Stateprovince'] == 'GA'), ['Timezone']] = 'America/Kentucky/Monticello'
    df.loc[(df['Stateprovince'] == 'DC'), ['Timezone']] = 'America/New_York'
    df.loc[(df['Stateprovince'] == 'NV'), ['Timezone']] = 'America/Los_Angeles'
    df.loc[(df['Stateprovince'] == 'MI'), ['Timezone']] = 'America/Detroit'
    df.loc[(df['Stateprovince'] == 'NC'), ['Timezone']] = 'America/New_York'
    df.loc[(df['Stateprovince'] == 'Arizona'), ['Timezone']] = 'America/Phoenix'
    df.loc[(df['Stateprovince'] == 'NJ'), ['Timezone']] = 'America/New_York'
    df.loc[(df['Stateprovince'] == 'OH'), ['Timezone']] = 'America/Indiana/Vevay'
    df.loc[(df['Stateprovince'] == 'UT'), ['Timezone']] = 'America/Shiprock'
    df.loc[(df['Stateprovince'] == 'Ca'), ['Timezone']] = 'America/Los_Angeles'
    df.loc[(df['Stateprovince'] == 'VA'), ['Timezone']] = 'America/New_York'
    df.loc[(df['Stateprovince'] == 'MD'), ['Timezone']] = 'America/New_York'
    df.loc[(df['Stateprovince'] == 'MN'), ['Timezone']] = 'America/Chicago'
    df.loc[(df['Stateprovince'] == 'IN'), ['Timezone']] = 'America/Indiana/Indianapolis'
    df.loc[(df['Stateprovince'] == 'CT'), ['Timezone']] = 'America/New_York'
    df.loc[(df['Stateprovince'] == 'HI'), ['Timezone']] = 'Pacific/Honolulu'
    df.loc[(df['Stateprovince'] == 'SC'), ['Timezone']] = 'America/New_York'
    df.loc[(df['Stateprovince'] == 'NH'), ['Timezone']] = 'America/New_York'
    df.loc[(df['Stateprovince'] == 'AZ'), ['Timezone']] = 'America/Phoenix'
    df.loc[(df['Stateprovince'] == 'ON'), ['Timezone']] = 'America/Toronto'
    df.loc[(df['Stateprovince'] == 'NM'), ['Timezone']] = 'America/Denver'
    df.loc[(df['Stateprovince'] == 'Ontario'), ['Timezone']] = 'America/Toronto'
    df.loc[(df['Stateprovince'] == 'Alberta'), ['Timezone']] = 'America/Edmonton'
    df.loc[(df['Stateprovince'] == 'BC'), ['Timezone']] = 'America/Vancouver'
    df.loc[(df['Stateprovince'] == 'TN'), ['Timezone']] = 'America/Chicago'
    df.loc[(df['Stateprovince'] == 'Florida'), ['Timezone']] = 'America/New_York'
    df.loc[(df['Stateprovince'] == 'WI'), ['Timezone']] = 'America/Toronto'
    df.loc[(df['Stateprovince'] == 'Texas'), ['Timezone']] = 'America/Indiana/Tell_City'
    df.loc[(df['Stateprovince'] == 'Pennsylvania'), ['Timezone']] = 'America/New_York'

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

Stan_clean()
matt_clean()

df['Comment'].fillna('UNKNOWN')
df['State'].fillna('UNKNOWN')

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
                    Valid Email BOOLEAN nullable==False,
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
