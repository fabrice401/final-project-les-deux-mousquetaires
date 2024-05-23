from flask import Flask, render_template, jsonify
# import boto3
# import dataset

# Create an instance of Flask class (represents our application)
# Pass in name of application's module (__name__ evaluates to current module name)
app = Flask(__name__, template_folder='./templates',static_folder='./templates/assets')
application = app  # AWS EB requires it to be called "application"


# Provide a landing page with some documentation on how to use API
@app.route("/")
def home():
    return render_template('overview.html')

@app.route("/overview.html")
def overview():
    return render_template('overview.html')

@app.route("/trends.html")
def trends():
    return render_template('trends.html')

@app.route("/network.html")
def network():
    return render_template('network.html')

if __name__ == "__main__":
    application.run()
