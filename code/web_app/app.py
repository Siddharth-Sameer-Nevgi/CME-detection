from flask import Flask, render_template, jsonify, request
import psycopg2
import pandas as pd
import re

app = Flask(__name__)

# DATABASE CONFIGURATION
DB_CONFIG = {
    "dbname": "solar_db",
    "user": "postgres",
    "password": "Abhyuday@postgresql",
    "host": "127.0.0.1", 
    "port": "5433"
}

def get_db_connection():
    conn = psycopg2.connect(**DB_CONFIG)
    return conn

@app.route('/')
def index():
    return render_template('index.html')

# API 1: Get the Raw Particle Data for the Graph
@app.route('/api/telemetry')
def get_telemetry():
    # We limit to the October Storm period for the demo
    start_date = request.args.get('start', '2024-05-10')
    end_date = request.args.get('end', '2024-10-15')
    
    conn = get_db_connection()
    # Fetch more parameters for a complete scientific dashboard
    # Downsampling: record_id % 5 to get more detail but keep it performant
    query = f"""
        SELECT observation_time, 
               proton_speed, 
               proton_density,
               proton_thermal_speed,
               alpha_density
        FROM swis_moments
        WHERE observation_time BETWEEN '{start_date}' AND '{end_date}'
        AND record_id % 5 = 0 
        ORDER BY observation_time ASC;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    
    # Calculate Alpha/Proton Ratio for the frontend
    # Avoid division by zero
    df['alpha_ratio'] = df.apply(
        lambda row: row['alpha_density'] / row['proton_density'] if row['proton_density'] > 0 else 0, 
        axis=1
    )
    
    # Convert Timestamp to String for JSON
    result = df.to_dict(orient='records')
    return jsonify(result)

# API 3: Get Data Time Range (For dynamic dashboard buttons)
@app.route('/api/metadata')
def get_metadata():
    conn = get_db_connection()
    # Find the absolute start and end of our dataset
    query = "SELECT MIN(observation_time) as min_date, MAX(observation_time) as max_date FROM swis_moments;"
    df = pd.read_sql(query, conn)
    conn.close()
    
    return jsonify({
        "min_date": df.iloc[0]['min_date'],
        "max_date": df.iloc[0]['max_date']
    })

# API 2: Get the 18 Detected Alerts
@app.route('/api/alerts')
def get_alerts():
    conn = get_db_connection()
    query = """
        SELECT generated_at, severity, message 
        FROM alerts 
        ORDER BY generated_at DESC;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    
    alerts_list = []
    for _, row in df.iterrows():
        start_time = row['generated_at']
        message = row['message']
        
        # Extract duration to calculate end_time
        # Format: "Duration: X days HH:MM:SS. Peak..."
        match = re.search(r"Duration: (.*?)\. Peak", message)
        end_time = start_time # Default fallback
        
        if match:
            try:
                duration = pd.to_timedelta(match.group(1))
                end_time = start_time + duration
            except:
                pass
        
        alerts_list.append({
            'generated_at': start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'severity': row['severity'],
            'message': row['message']
        })
        
    return jsonify(alerts_list)

if __name__ == '__main__':
    app.run(debug=True)