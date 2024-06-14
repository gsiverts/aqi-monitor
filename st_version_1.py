import streamlit as st
import pandas as pd
import altair as alt
import sqlite3
import datetime
import time

# Database connection
conn = sqlite3.connect('iotdata.db')

# Function to fetch and process data for a specific column
@st.cache_data(ttl=60)  # Cache the data for 60 seconds to avoid unnecessary queries
def get_data():
    with conn:
        df = pd.read_sql_query(
            f"SELECT timestamp, aqi, temp, rh, hpa, light FROM aqi ORDER BY timestamp DESC LIMIT 1440", conn
        )
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    for col in ['aqi', 'temp', 'rh', 'hpa', 'light']:
        df[col] = df[col].rolling(window=3, center=True).median()  # Apply median filter
    return df

# Streamlit App
st.title('Local AQI and Environmental Monitor')
st.subheader("Realtime Local AQI Chart, Updates Every Minute")

# Update every minute
placeholder_dict = {col: st.empty() for col in ['aqi', 'temp', 'rh', 'hpa', 'light']}
while True:
    df = get_data()

    # Create charts for each variable
    for column in ['aqi', 'temp', 'rh', 'hpa', 'light']:
        
            # Filter data for the specific column and melt for plotting
            df_melted = df[['timestamp', column]].melt('timestamp', var_name='category', value_name='value')

            # Calculate dynamic y-axis range for this column
            y_min = df_melted['value'].min()
            y_max = df_melted['value'].max()
            y_range = y_max - y_min
            y_min_scaled = max(0, y_min - 0.30 * y_range)  
            y_max_scaled = y_max + 0.30 * y_range

            # Create Altair chart for the variable
            chart = alt.Chart(df_melted, title=f"{column}").mark_line(point=False).encode(
                x=alt.X('timestamp:T', axis=alt.Axis(title='Timestamp', titleFontSize=12)),
                y=alt.Y('value:Q', scale=alt.Scale(domain=[y_min_scaled, y_max_scaled]), axis=alt.Axis(title=column, titleFontSize=12)),
                color='category:N',
                tooltip=['timestamp:T', 'category:N', 'value:Q']
            ).properties(
                width=600,
                height=400
            ).interactive()

            # Display the chart in the placeholder
            placeholder_dict[column].altair_chart(chart, use_container_width=True)

    time.sleep(60)  # Refresh every 60 seconds

