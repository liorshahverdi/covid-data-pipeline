import main
import pytest
import psycopg2
import pandas as pd

def test_connect():
	cursor = main.connect()
	assert type(cursor) == psycopg2.extensions.cursor
	cursor.execute('select * from deaths')
	result = cursor.fetchall()
	assert type(result) == list

def test_get_county_with_max_deaths_by_state():
	data = {
		'state': ['New York', 'New York', 'Los Angeles'],
		'county': ['Clark', 'Cayuga', 'Oklahoma'],
		'deaths': [900, 3000, 12000],
		'date': ['2022-09-28' for i in range(3)]
	}
	df = pd.DataFrame(data=data)
	result_dict = main.get_county_with_max_deaths_by_state(df, 'New York')
	assert type(result_dict) == dict
	assert result_dict == {'county': 'Cayuga', 'date': '2022-09-28', 'deaths': 3000, 'state': 'New York'}

def test_get_live_county_data():
	df = main.get_live_county_data()
	assert type(df) == pd.DataFrame
	assert len(df) > 10

def test_write_to_db():
	cursor = main.connect()
	cursor.execute("DELETE FROM DEATHS where state = 'Test State'")
	test_dict = {}
	with pytest.raises(Exception) as exception:
		result = main.write_to_db(test_dict, cursor)
	test_dict = {
		'county': 'Somewhen',
		'date': '2022-09-28',
		'deaths': 4000,
		'state': 'Test State'
	}
	main.write_to_db(test_dict, cursor)
	cursor.execute("SELECT county FROM DEATHS where county = 'Somewhen'")
	assert cursor.fetchone()[0] == 'Somewhen'

def test_select_from_db():
	cursor = main.connect()
	result = main.select_from_db(cursor)[0]
	assert type(result) == str
	assert result != ''
