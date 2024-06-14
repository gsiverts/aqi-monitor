import paho.mqtt.subscribe as subscribe
import json
import pandas as pd
import numpy as np
import sqlite3
import datetime
from datetime import datetime
import pytz

#sqlite_web --host 0.0.0.0 iotdata.db
	
# AQI Calculation Function (May 2024 breakpoints)
def calculate_aqi(pm25_concentration):
	breakpoints = pd.DataFrame({
		'Concentration': [0.0, 9.0, 35.4, 55.4, 125.4, 225.4, 350.4, 500.4],
		'AQI': [0, 50, 100, 150, 200, 300, 400, 500]
	})

	if pm25_concentration < 0:
		return None
	elif pm25_concentration > 500.4:
		return 500

	idx = np.searchsorted(breakpoints['Concentration'], pm25_concentration)
	lower_breakpoint = breakpoints.iloc[idx - 1]
	upper_breakpoint = breakpoints.iloc[idx]

	slope = (upper_breakpoint['AQI'] - lower_breakpoint['AQI']) / \
			(upper_breakpoint['Concentration'] - lower_breakpoint['Concentration'])
	aqi = int(lower_breakpoint['AQI'] + slope * (pm25_concentration - lower_breakpoint['Concentration']))
	return aqi
	
tz = pytz.timezone('America/Denver')
conn = sqlite3.connect('iotdata.db')
hostname = 'localhost'
port = 1883

# Clear table data
#with conn:  # Use a context manager for automatic transaction handling
#    cursor = conn.cursor()
#    cursor.execute("DELETE FROM aqi;")

# Perform VACUUM outside of the transaction
#conn.execute("VACUUM;")  


def on_message_print(client, userrdata, message):

	data = json.loads(message.payload.decode('utf-8'))		
	#print(data)
	formatted_json = json.dumps(data, indent=4, sort_keys=True)
	print(formatted_json)
	
	local_timezone = pd.Timestamp.now().tz
	now = pd.Timestamp.now(tz=local_timezone)
	
	aqi = calculate_aqi(data['pms25'])
	
	columns = ['timestamp', 'aqi', 'temp', 'gas', 'rh', 'hpa', 'alt', 'light', 'cell_v', 'cell_p', 'pms10', 'pms25', 'pms100', 'pme10', 'pme25',
	'pme_100', 'pc03', 'pc05', 'pc10', 'pc25', 'pc50', 'pc100']
	
	values = [ now, aqi, data['temp'], data['gas'], data['rh'], data['hpa'], data['alt'], data['light'],
			   data['cell_v'], data['cell_p'],
	  		   data['pms10'], data['pms25'], data['pms100'],
	  		   data['pme10'], data['pme25'], data['pme100'], 
	  		   data['pc03'], data['pc05'], data['pc10'], data['pc25'], data['pc50'], data['pc100'] ]
	
	df = pd.DataFrame.from_records([values], columns=columns)  	
	df.to_sql('aqi', conn, if_exists='append', index=False) 
	conn.commit()
    
subscribe.callback(on_message_print, "#", hostname=hostname, port=port, userdata={"message_count": 0})