import os, sys, io, requests
from datetime import datetime, timedelta
import pandas as pd
import psycopg2

sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

def connect():
	"""
		Connects to a local postgres instance
		running on port 5432.
		TODO:
			- read host, database, user, password, port from
			 config file.

		Returns a connection cursor to the Postgres instance.
	"""
	conn = psycopg2.connect(
		host='localhost',
		database='covid',
		user='import',
		password='postgres',
		port=5432)
	conn.autocommit = True
	return conn.cursor()

def todays_date(df: pd.DataFrame) -> None:
	"""
		Params:
			df : pandas DataFrame
				- expected to contain a date column.
		Gets the first row from the DataFrame.
		Prints a nice title with the live data's date to stdout.

		Returns None.
	"""
	result_row = df.head(1).iloc[0]
	print (f"COUNTY WITH MOST DEATHS BY STATE FOR {result_row['date']}")

def get_county_with_max_deaths_by_state(df: pd.DataFrame, state: str) -> dict:
	"""
		Params:
			df: pandas DataFrame
			state: String
		Filters the rows in the DataFrame to only the rows for the given state.
		Sort the resulting rows in the DataFrame by deaths in descending order,
		then gets the first one in the list, (essentially the county with the most deaths that day)
		grabs the columns for state, county, deaths, and date and returns them in a dictionary.

		Returns dictionary containing values for state, county, deaths and date.
	"""
	result_df = df[df['state'] == state].sort_values(by='deaths', ascending=False)
	result_row = result_df.head(1).iloc[0]
	state, county, deaths, date = result_row['state'], result_row['county'], result_row['deaths'], result_row['date']
	#print(f"State: {state}, County: {county}, Deaths: {deaths}, Date: {date}")
	return {'state': state, 'county': county, 'deaths': deaths, 'date': date}

def get_live_county_data() -> pd.DataFrame:
	"""
		Params:
			None
		Makes a GET request to grab NYT live county data csv.
		
		Returns pandas DataFrame with the resulting csv.
	"""
	filepath = f"us-counties.csv"
	url = f"https://raw.githubusercontent.com/nytimes/covid-19-data/master/live/{filepath}"
	response = requests.get(url).content
	return pd.read_csv(io.StringIO(response.decode('utf-8')))

def write_to_db(result_dict: dict, cur: psycopg2.extensions.cursor) -> None:
	"""
		Params:
			result_dict: dictionary
			cur: psycopg2 cursor
		
		Writes the result_dict values into a Postgres table called deaths.

		Returns None.
	"""
	keys = ['state', 'county', 'deaths', 'date']
	if all([key in result_dict for key in keys]):
		query = f"INSERT INTO deaths VALUES ('{result_dict['state']}', '{result_dict['county']}', {result_dict['deaths']}, '{result_dict['date']}')"
		try:
			cur.execute(query)
			print (query)
		except:
			print (f'There is already a record for {result_dict["state"]} on {result_dict["date"]}. Skipping.')
	else:
		raise Exception(f'result_dict is missing keys. result_dict: {result_dict}')

def select_from_db(cur: psycopg2.extensions.cursor) -> None:
	"""
		Params:
			cur: pyscopg2 cursor

		Retrieves all rows from the Postgres deaths table.
		Not currently being used but good for a sanity check.
		
		Returns None
	"""
	cur.execute("SELECT * FROM deaths")
	return cur.fetchone()

"""
df = get_live_county_data()
todays_date(df)
state_names = ["Alaska", "Alabama", "Arkansas", "American Samoa", "Arizona", "California", "Colorado", "Connecticut", "District of Columbia", "Delaware", "Florida", "Georgia", "Guam", "Hawaii", "Iowa", "Idaho", "Illinois", "Indiana", "Kansas", "Kentucky", "Louisiana", "Massachusetts", "Maryland", "Maine", "Michigan", "Minnesota", "Missouri", "Mississippi", "Montana", "North Carolina", "North Dakota", "Nebraska", "New Hampshire", "New Jersey", "New Mexico", "Nevada", "New York", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Puerto Rico", "Rhode Island", "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah", "Virginia", "Virgin Islands", "Vermont", "Washington", "Wisconsin", "West Virginia", "Wyoming"]

cur = connect()
for state in state_names:
	write_to_db(get_county_with_max_deaths_by_state(df, state), cur)

select_from_db(cur)
"""

def extract():
	df = get_live_county_data()
	df.to_csv('daily.csv')

def transform(df: pd.DataFrame, state: str, cur: psycopg2.extensions.cursor):
	write_to_db(get_county_with_max_deaths_by_state(df, state), cur)	

def load(state_names: list):
	df = pd.read_csv('daily.csv')
	cur = connect()
	for state in state_names:
		transform(df, state, cur)


if __name__ == '__main__':
	extract()
	state_names = ["Alaska", "Alabama", "Arkansas", "American Samoa", "Arizona", "California", "Colorado", "Connecticut", "District of Columbia", "Delaware", "Florida", "Georgia", "Guam", "Hawaii", "Iowa", "Idaho", "Illinois", "Indiana", "Kansas", "Kentucky", "Louisiana", "Massachusetts", "Maryland", "Maine", "Michigan", "Minnesota", "Missouri", "Mississippi", "Montana", "North Carolina", "North Dakota", "Nebraska", "New Hampshire", "New Jersey", "New Mexico", "Nevada", "New York", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Puerto Rico", "Rhode Island", "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah", "Virginia", "Virgin Islands", "Vermont", "Washington", "Wisconsin", "West Virginia", "Wyoming"]
	load(state_names)
	os.remove('daily.csv')
