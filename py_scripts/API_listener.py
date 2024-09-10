import flask 
from flask import request, render_template
from API_app import process_request, report_by_quarter_get_data, report_hired_employees_with_avg

app = flask.Flask(__name__, template_folder='templates')
app.config["DEBUG"] = True

######
# End point for bulk insert
# The data must be in JSON format in the body request
# Format:
#       {   
#           "0" : {
#                     "name" : "table_name",
#                     "row_data" : {
#                                       "0" : { "col1_name" : "col1_value", ... , "col2_name" : "col2_value" }
#                                       ...
#                                       "n" : { "col1_name" : "col1_value", ... , "col2_name" : "col2_value" }
#                                   }
#                 }            
#       }   
#######  
@app.route("/bulk/tables",  methods = ['POST'])
def api_insert():
    bulk_data = request.get_json()
    processed_count, unprocessed_count = process_request(bulk_data)
    return { 'rows_inserted' : processed_count, 'rows_failed' : unprocessed_count }


#######
# End point to get the report for employees hired on each quarter by area
# Return value: HTML file
#######
@app.route("/features/employees-hired-by-department-job",  methods = ['GET'])
def api_employees_hired_by_department_job():
    df = report_by_quarter_get_data()
    return render_template("api_employees_hired_by_department_job.html", rows=df)

#######
# End point to get the report for departments with a higher amount of hiring than 
# the company average
# Return value: HTML file
#######
@app.route("/features/hired-employees-with-avg",  methods = ['GET'])
def api_hired_employees_with_avg():
    df = report_hired_employees_with_avg()
    return render_template("api_hired_employees_with_avg.html", rows=df)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=1500)