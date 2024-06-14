import os
import paho.mqtt.subscribe as subscribe
import json
import base64
import pandas as pd
import sqlite3
from tabulate import tabulate
import time
import datetime

from datetime import datetime
import pytz

# Start sqlite-web from this directory
# sqlite_web --host 0.0.0.0 iotdata.db

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

conn = sqlite3.connect('iotdata.db')
 
hostname = 'nam1.cloud.thethings.network'
port = 1883
auth = {'username': 'vole-monitor@ttn',
        'password': 'NNSXS.5UPJRN6FPNJBBLX5VZJYGKKVURHS3DHUEZX5I2A.YXNYD3AMHDM4WDPC7DKDEECIHUCL37CRU5Z2EGKKGMB5NV5MP6JA'}
        
traps = {'70B3D57ED0066F3E': 'dev_3E', # esp32s3 LORA cpu_temp, batt, detection, light/deep sleep
         '70B3D57ED0066FB5': 'dev_B5', # Stick
         '70B3D57ED00670FF': 'dev_FF'} # Stick Lite

tz = pytz.timezone('America/Denver')

def on_message_print(client, userdata, message):
    
    try:
        data = json.loads(message.payload.decode('utf-8'))
                
        timestamp = data['received_at']
        device = traps[data['end_device_ids']['dev_eui']]  
        decoded_bytes  = base64.b64decode(data['uplink_message']['frm_payload'])
        
        if device == 'dev_FF' or device == 'dev_B5'or device == 'dev_3E':
           print()
           
           # Extract individual values
           counter = decoded_bytes[0]
           temperature = decoded_bytes[1] - 100
           battery_level = decoded_bytes[2]
           motion = decoded_bytes[3]
              
           rssi = data['uplink_message']['rx_metadata'][0]['rssi']
           snr = data['uplink_message']['rx_metadata'][0]['snr']

           local_timezone = pd.Timestamp.now().tz
           now = pd.Timestamp.now(tz=local_timezone)
           
           row = {'timestamp': now, #timestamp_loc, 
                  'device': device, 
                  'temperature': temperature, 
                  'battery_level': battery_level,
                  'motion': motion,
                  'rssi': rssi, 
                  'snr': snr}
                        
           columns = ['timestamp', 'device', 'temperature', 'battery_level', 'motion',' rssi', 'snr']
           values = [row['timestamp'], device, temperature, battery_level, motion, rssi, snr]
           df = pd.DataFrame.from_records([values], columns=columns)
           print(df)  
           df.to_sql('voles', conn, if_exists='append', index=False)
           conn.commit()
               
           table_name = "voles"
           timestamp_column = "timestamp"
           limit = 20
           
           query = f"""
           SELECT *
           FROM {table_name}
           ORDER BY {timestamp_column} ASC 
           LIMIT {limit}
           OFFSET (SELECT COUNT(*) FROM {table_name}) - {limit};
           """
           
           df = pd.read_sql_query(query, conn)
           print(tabulate(df, headers='keys', tablefmt='psql'))
           time.sleep(1)
           
    except:
        # this is usually a join accept downlink, but look at the packet to make sure
        #formatted_json = json.dumps(data, indent=4, sort_keys=True)
        #print(formatted_json)
        print("Join accept downlink from TTN")
                       
subscribe.callback(on_message_print, "#", hostname=hostname, port=port, auth=auth, userdata={"message_count": 0})

