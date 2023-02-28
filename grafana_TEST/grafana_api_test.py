from flask import Flask, make_response, jsonify, request
import database_api_test
import json
from calendar import timegm
from datetime import datetime

app = Flask(__name__)

def convert_to_time_ms(timestamp):
    return 1000 * timegm(datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S').timetuple())


app.make_response("Hello there")
@app.route('/')
def hello_world():
    return 'Hello World!'

# we will put as targets the metrics we want to check (raws and aggrs)
@app.route('/search', methods=['POST'])
def search():
  return jsonify([
    'TH1_raw',
    'TH2_raw',
    'HVAC1_raw',
    'HVAC2_raw',
    'MiAC1_raw',
    'MiAC2_raw',
    'Etot_raw',
    'MOV1_raw',
    'W1_raw',
    'Wtot_raw',
    'TH1_aggr',
    'TH2_aggr',
    'HVAC1_aggr',
    'HVAC2_aggr',
    'MiAC1_aggr',
    'MiAC2_aggr',
    'MOV1_aggr',
    'W1_aggr'
  ])

@app.route('/query', methods=['POST'])
def query():
    req = request.get_json()
    result = []

    # get result from databse-api
    results_db = database_api_test.get_data(req['targets'][0]['target'])

    data = [
        {
            "target": req['targets'][0]['target'],
            "datapoints": []
        }
    ]
    for result in results_db:
        data[0]["datapoints"].append([float(result["value"]), convert_to_time_ms(result["time"])])


    return jsonify(data)


if __name__ == '__main__':
    app.run(host='localhost', port=5000)