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

# we will put as targets the metrics we want to check
@app.route('/search', methods=['POST'])
def search():
  return jsonify(['my_series', 'another_series'])

@app.route('/query', methods=['POST'])
def query():
    req = request.get_json()
    result = []

    # get result from databse-api
    results_db = database_api_test.get_dummy_sum()

    data = [
        {
            "target": req['targets'][0]['target'],
            "datapoints": [
            ]
        }
    ]
    for result in results_db:
        data[0]["datapoints"].append([float(result["value"]), convert_to_time_ms(result["time"])])


    return jsonify(data)


if __name__ == '__main__':
    app.run(host='localhost', port=5000)