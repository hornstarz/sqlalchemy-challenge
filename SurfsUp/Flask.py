from flask import Flask, jsonify
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, func
from sqlalchemy.orm import Session
from sqlalchemy.ext.automap import automap_base

app = Flask(__name__)

# Paths to your data files
database_path = r"C:\Users\zacha\OneDrive\Desktop\sqlalchemy-challenge\SurfsUp\Resources\hawaii.sqlite"
measurements_df = pd.read_csv(r"C:\Users\zacha\OneDrive\Desktop\sqlalchemy-challenge\SurfsUp\Resources\hawaii_measurements.csv")
stations_df = pd.read_csv(r"C:\Users\zacha\OneDrive\Desktop\sqlalchemy-challenge\SurfsUp\Resources\hawaii_stations.csv")

# Create engine for SQLite database
database_path = r"C:\\Users\\zacha\\OneDrive\\Desktop\\sqlalchemy-challenge\\SurfsUp\\Resources\\hawaii.sqlite"
engine = create_engine(f"sqlite:///{database_path}")

# Load data into variables for easy reference
precipitation_data = measurements_df[['date', 'prcp']].dropna()
temperature_data = measurements_df[['date', 'station', 'tobs']].dropna()
stations_data = stations_df

# Making sure the date objects are loaded properly
measurements_df['date'] = pd.to_datetime(measurements_df['date'], format='%Y-%m-%d', errors='coerce')

# Reflect tables
Base = automap_base()
Base.prepare(engine, reflect=True)

# Mapped classes
Measurement = Base.classes.measurement
Station = Base.classes.station

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
    session = Session(engine)
    try:
        # Get the latest date in the dataset
        last_date = session.query(func.max(Measurement.date)).scalar()
        one_year_ago = datetime.strptime(last_date, '%Y-%m-%d') - timedelta(days=365)

        # Query precipitation data for the past year
        results = session.query(Measurement.date, Measurement.prcp).filter(Measurement.date >= one_year_ago, Measurement.prcp.isnot(None) ).all()
        
        # Debugging
        print(f"Last Date: {last_date}") 
        print(f"One Year Ago: {one_year_ago}") 
        print(f"Query Results: {results}")
        session.close()

        # Format the results into a dictionary
        precip_dict = {date: prcp for date, prcp in results}
        return jsonify(precip_dict)
    except Exception as e:
        session.close()
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500


# 3. Stations Route
@app.route('/api/v1.0/stations')
def stations():
    session = Session(engine)
    try:
        # Query all stations
        results = session.query(Station.station).all()
        print(f"Stations Query Results: {results}")
        session.close()

        # Flatten results into a list
        station_list = [station[0] for station in results]
        return jsonify(station_list)
    except Exception as e:
        session.close()
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500

# 4. Temperature Observations (TOBS) Route
@app.route('/api/v1.0/tobs')
def tobs():
    session = Session(engine)
    try:
        # Get the most active station
        most_active_station = session.query(Measurement.station).group_by(Measurement.station).order_by(func.count().desc()).first()[0]

        # Get the latest date and calculate one year back
        last_date = session.query(func.max(Measurement.date)).scalar()
        one_year_ago = datetime.strptime(last_date, '%Y-%m-%d') - timedelta(days=365)

        # Query temperature observations for the most active station
        results = session.query(Measurement.date, Measurement.tobs).filter(
            Measurement.station == most_active_station,
            Measurement.date >= one_year_ago
        ).all()
        session.close()

        # Format results into a list of dictionaries
        tobs_list = [{"date": date, "temperature": tobs} for date, tobs in results]
        return jsonify(tobs_list)
    except Exception as e:
        session.close()
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500
    
    tobs_list = filtered_data['tobs'].tolist()
    return jsonify(tobs_list)

@app.route('/api/v1.0/<start>')
@app.route('/api/v1.0/<start>/<end>')
def temp_range(start, end=None):
    session = Session(engine)
    try:
        # Query temperature statistics
        if end:
            results = session.query(
                func.min(Measurement.tobs),
                func.avg(Measurement.tobs),
                func.max(Measurement.tobs)
            ).filter(Measurement.date >= start).filter(Measurement.date <= end).all()
        else:
            results = session.query(
                func.min(Measurement.tobs),
                func.avg(Measurement.tobs),
                func.max(Measurement.tobs)
            ).filter(Measurement.date >= start).all()

        session.close()

        # Format results into a dictionary
        stats = {
            "Min Temp": results[0][0],
            "Avg Temp": results[0][1],
            "Max Temp": results[0][2]
        }
        return jsonify(stats)
    except Exception as e:
        session.close()
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500

# Run the application
if __name__ == '__main__':
    app.run(debug=True)
