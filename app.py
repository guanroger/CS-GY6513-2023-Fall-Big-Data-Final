from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from cluster import get_spark_context, get_data, extract_coords, calculate_center, get_hotspots
from geopy.geocoders import Nominatim

app = Flask(__name__)
# May need to change this depending on where you are running the code
DATA = [
    './content/taxi_data_2023/yellow_tripdata_2023-01.parquet', 
    './content/taxi_data_2023/yellow_tripdata_2023-02.parquet',
    './content/taxi_data_2023/yellow_tripdata_2023-03.parquet',
    './content/taxi_data_2023/yellow_tripdata_2023-04.parquet',
    './content/taxi_data_2023/yellow_tripdata_2023-05.parquet',
    './content/taxi_data_2023/yellow_tripdata_2023-06.parquet',
    './content/taxi_data_2023/yellow_tripdata_2023-07.parquet',
    './content/taxi_data_2023/yellow_tripdata_2023-08.parquet',
    './content/taxi_data_2023/yellow_tripdata_2023-09.parquet',
]
ZONES = './content/taxi_zones.csv'
SEED = 42

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/search', methods=['GET', 'POST']) # line 67 in index.hmtl
def search():
    # first parse the post request to get the date and time
    if request.method == 'POST':
        borough = request.form.get('demo-category')
        address = request.form.get('demo-address')
        zip_code = request.form.get('demo-zip')
        date = request.form.get('demo-date')

        time = request.form.get('demo-time')  # This is the time field
        time = time + ":00" # to match the format that the function needs

        coords = convert_address_to_coords(address, borough, zip_code)
        if coords == (None, None):
            #TODO: error handling on frontend
            #TODO: also figure out mongoDB stuff
            pass
        cluster_info = get_hotspots(time, coords, borough)
        print(cluster_info)

        return cluster_info

def convert_address_to_coords(address, borough, zip_code):
    """
        address is a string
        returns a tuple of (longitude, latitude)
        https://medium.com/@hazallgultekin/convert-address-to-latitude-longitude-using-python-21844da3d032
    """
    address = f"{address}, {borough}, New York, NY {zip_code}"
    # Initialize Nominatim API
    geolocator = Nominatim(user_agent="taxi_app")

    # Geocode the address
    location = geolocator.geocode(address)

    # Check if location is found
    if location:
        return (location.latitude, location.longitude)
    else:
        return None, None




def parse_cluster_info(cluster_info):
    """
        cluster_info is an array of tuples
        each tuple has info in the order of:
        x['Centroid Longitude'], x['Centroid Latitude'], x['Average Revenue'], x['Average Tip'])
    """
    pass


if __name__ == '__main__':
    app.run(debug=True)
