import flask
from flask import request
from API_app import process_request

app = flask.Flask(__name__)
app.config["DEBUG"] = True
        
@app.route("/api/v1/books",  methods = ['POST'])
def api_insert():
    bulk_data = request.get_json()
    processed_count, unprocessed_count = process_request(bulk_data)
    return { 'rows_inserted' : processed_count, 'rows_failed' : unprocessed_count }
if __name__ == '__main__':
    app.run(host="0.0.0.0", port=1500)