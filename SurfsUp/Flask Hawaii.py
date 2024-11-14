#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov 14 10:35:48 2024

@author: zachhorn
"""

from flask import Flask, jsonify, request
import pandas as pd
from datetime import datetime, timedelta
import sqlite3
import os

desktop = os.path.join(os.path.expanduser("~"), "Desktop")
filePath = os.path.join(desktop, "hawaii_measurements.csv")
filePath2 = os.path.join(desktop, "hawaii_stations.csv")


app = Flask(__name__)

# Load the data

DATABASE = '/mnt/data/hawaii.sqlite'
stations_data = pd.read_csv('/Users/zachhorn/Desktop/sqlalchemy-challenge/SurfsUp/Resources')           
temperature_data = pd.read_csv('/Users/zachhorn/Desktop/sqlalchemy-challenge/SurfsUp/Resources')      

# 1. Home Route 
@app.route('/')
def home():
    return jsonify({
        "available_routes": [
            "/api/v1.0/precipitation",
            "/api/v1.0/stations",
            "/api/v1.0/tobs",
            "/api/v1.0/<start>",
            "/api/v1.0/<start>/<end>"
        ]
    })

# 2. Precipitation Route
@app.route('/api/v1.0/precipitation')
def precipitation():
    # Filter data for the last 12 months
    last_date = precipitation_data['date'].max()
    last_date = datetime.strptime(last_date, '%Y-%m-%d')
    one_year_ago = last_date - timedelta(days=365)
    
    filtered_data = precipitation_data[precipitation_data['date'] >= one_year_ago.strftime('%Y-%m-%d')]
    
    # Convert to dictionary: date as key, prcp as value
    precip_dict = filtered_data.set_index('date')['prcp'].to_dict()
    return jsonify(precip_dict)

# 3. Stations Route
@app.route('/api/v1.0/stations')
def stations():
    stations_list = stations_data['station'].tolist()
    return jsonify(stations_list)

# 4. Temperature Observations (TOBS) Route
@app.route('/api/v1.0/tobs')
def tobs():
    # Identify the most active station
    most_active_station = temperature_data['station'].value_counts().idxmax()
    
    # Filter data for the most active station for the past year
    last_date = temperature_data['date'].max()
    last_date = datetime.strptime(last_date, '%Y-%m-%d')
    one_year_ago = last_date - timedelta(days=365)
    
    filtered_data = temperature_data[
        (temperature_data['station'] == most_active_station) & 
        (temperature_data['date'] >= one_year_ago.strftime('%Y-%m-%d'))
    ]
    
    tobs_list = filtered_data['tobs'].tolist()
    return jsonify(tobs_list)

# 5. Start Date Route
@app.route('/api/v1.0/<start>')
def start(start):
    # Filter temperature data for dates greater than or equal to start
    filtered_data = temperature_data[temperature_data['date'] >= start]
    
    # Calculate TMIN, TAVG, TMAX
    stats = {
        "TMIN": filtered_data['tobs'].min(),
        "TAVG": filtered_data['tobs'].mean(),
        "TMAX": filtered_data['tobs'].max()
    }
    return jsonify(stats)

# 6. Start-End Date Range Route
@app.route('/api/v1.0/<start>/<end>')
def start_end(start, end):
    # Filter temperature data for the date range
    filtered_data = temperature_data[(temperature_data['date'] >= start) & (temperature_data['date'] <= end)]
    
    # Calculate TMIN, TAVG, TMAX
    stats = {
        "TMIN": filtered_data['tobs'].min(),
        "TAVG": filtered_data['tobs'].mean(),
        "TMAX": filtered_data['tobs'].max()
    }
    return jsonify(stats)

# Run the application
if __name__ == '__main__':
    app.run(debug=True)
