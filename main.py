import io, requests
from datetime import datetime, timedelta
import pandas as pd
import psycopg2

def connect():
	conn = psycopg2.connect(
		host='localhost',
		database='covid',
		user='import',
		password='postgres',
		port=5432)
	return conn.cursor()

def todays_date(df):
	result_row = df.head(1).iloc[0]
	print (f"COUNTY WITH MOST DEATHS BY STATE FOR {result_row['date']}")

def get_county_with_max_deaths_by_state(df, state):
	result_df = df[df['state'] == state].sort_values(by='deaths', ascending=False)
	result_row = result_df.head(1).iloc[0]
	state, county, deaths, date = result_row['state'], result_row['county'], result_row['deaths'], result_row['date']
	#print(f"State: {state}, County: {county}, Deaths: {deaths}, Date: {date}")
	return {'state': state, 'county': county, 'deaths': deaths, 'date': date}

def get_live_county_data():
	filepath = f"us-counties.csv"
	url = f"https://raw.githubusercontent.com/nytimes/covid-19-data/master/live/{filepath}"
	response = requests.get(url).content
	return pd.read_csv(io.StringIO(response.decode('utf-8')))

def write_to_db(result_dict, cur):
	query = f"INSERT INTO deaths VALUES ('{result_dict['state']}', '{result_dict['county']}', {result_dict['deaths']}, '{result_dict['date']}')"
	print (query)
	cur.execute(query)

def select_from_db(cur):
	cur.execute("SELECT * FROM deaths")
	print (cur.fetchone()) 

df = get_live_county_data()
todays_date(df)
state_names = ["Alaska", "Alabama", "Arkansas", "American Samoa", "Arizona", "California", "Colorado", "Connecticut", "District of Columbia", "Delaware", "Florida", "Georgia", "Guam", "Hawaii", "Iowa", "Idaho", "Illinois", "Indiana", "Kansas", "Kentucky", "Louisiana", "Massachusetts", "Maryland", "Maine", "Michigan", "Minnesota", "Missouri", "Mississippi", "Montana", "North Carolina", "North Dakota", "Nebraska", "New Hampshire", "New Jersey", "New Mexico", "Nevada", "New York", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Puerto Rico", "Rhode Island", "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah", "Virginia", "Virgin Islands", "Vermont", "Washington", "Wisconsin", "West Virginia", "Wyoming"]

cur = connect()
for state in state_names:
	write_to_db(get_county_with_max_deaths_by_state(df, state), cur)

