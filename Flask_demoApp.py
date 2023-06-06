# -*- coding: utf-8 -*-
"""
Created on Sun Jun  4 11:48:14 2023

@author: krishna
"""
from flask import Flask, request, redirect
import subprocess, sys, os
from flask import Flask, render_template_string

cwd = os.getcwd()

app = Flask(__name__)

@app.route('/')
def index():
    '''
    routing func to display the home page

    Returns
    -------
    html : string
        html home page.

    '''
    # Create the HTML for the page
    html = """<html><head><style>
    body { background-color: #02197d; background-image: url("/static/photos/background.gif"); background-position: bottom;  background-size: 100% 100%;}
    form { padding: 20px; }
    input[type='submit'] { background-color: #00cc99; color: #ffffff; font-size: 16px; padding: 10px 20px; border: none; }
    input[type='submit']:hover { background-color: #ffffff; }
    </style></head><body><form action='/kafkaProducer' method='POST'>
    <p style="color:white;">Hello, i am Groot!<br>\
            <br>This is a demo project on kafka and python !<br>
            <br>Thanks and Regards,\
            <br>Groot Yadav</p>
    <br>
    <input type='submit' value='Kafka Producer'>  \t """
    
    return html

	
@app.route('/kafkaProducer', methods=['POST'])
def kafkaProducer():
    '''
    func to call kafka producer over subprocess

    Returns
    -------
    html : string
        renders output from subprocess python call.

    '''
    # Run the external Python script and capture its output
    try:
        output = subprocess.check_output(['python', 'kafka_senti_producer.py','producer']).decode("utf-8")
    except subprocess.CalledProcessError as error:
        print("Error in kafka producer endpoint func")
        print (error.output.decode("utf-8"))
        output = error.output.decode("utf-8")

    # Create the HTML for the page
    html = "<html><head><style>"
    html += "body { background-color: #02197d; }"
    html += "form { background-color: #ffffff; padding: 20px; }"
    html += "input[type='submit'] { background-color: #00cc99; color: #ffffff; font-size: 16px; padding: 10px 20px; border: none; }"
    html += "input[type='submit']:hover { background-color: #009966; }"
    html += "textarea { width: 100%; height: 300px; }"
    html += "</style></head><body><form action='/kafkaConsumer' method='POST'>"
    html += "<textarea id='error_message'>" + output + "</textarea>"
    
    html += '<input type="submit" value="Kafka Consumer">'
    html += "</form></body></html>"
    
    return html
	
	

@app.route('/kafkaConsumer', methods=['POST'])
def kafkaConsumer():
    '''
    Func to call kafka consumer python sub process
    performs sentiment analysis on batch
    and saves output charts

    Returns
    -------
    html : string
        renders output of kafka consumer process.

    '''
    # Run the external Python script and capture its output
    try:
        output = subprocess.check_output(['python', 'kafka_senti_consumer.py', 'consumer']).decode("utf-8")
    except subprocess.CalledProcessError as error:
        output = error.output.decode("utf-8")

    # Create the HTML for the page
    html = "<html><head><style>"
    html += "body { background-color: #02197d; }"
    html += "form { background-color: #ffffff; padding: 20px; }"
    html += "input[type='submit'] { background-color: #00cc99; color: #ffffff; font-size: 16px; padding: 10px 20px; border: none; }"
    html += "input[type='submit']:hover { background-color: #009966; }"
    html += "textarea { width: 100%; height: 80%; }"
    html += "</style></head><body><form action='/sentimentOutput' method='POST'>"
    html += "<textarea id='error_message'>" + output + "</textarea>"
    html += '<input type="submit" value="Sentiment Result">'
    html += "</form></body></html>"
    
    return html

@app.route('/sentimentOutput', methods=['POST'])
def sentimentOutput():
    '''
    func to render results
    for sentiment analysis

    Returns
    -------
    TYPE
        DESCRIPTION.

    '''
    # create html for the page
    html = "<html><head><title>Kafka Sentiment Analysis output</title><style>"
    html += "body { background-color: #02197d; }"
    html += "form { background-color: #ffffff; padding: 20px; }"
    html += "input[type='submit'] { background-color: #00cc99; color: #ffffff; font-size: 16px; padding: 10px 20px; border: none; }"
    html += "input[type='submit']:hover { background-color: #009966; }"
    html += "</style></head><body><form action='/go_home' method='POST'><h1>Sentiment Pie chart</h1>"
    filename = "pieplot.png"
    html += f'<img src="{{{{ url_for(\'static\', filename=\'{filename}\') }}}}" alt="{filename}"><br>'

    exp_dict = {'neg':'Negative words', 'pos':'Positive words', 'neu':'Neutral words'}
    for filename in [ "wordcloud_neg.png", "wordcloud_pos.png", "wordcloud_neu.png"]:
        #html += f'<img src="{filename}" alt="{filename}">'
        html += "<h1>Word Cloud - {}</h1>".format(exp_dict[filename.split(".")[0].split("_")[-1]])
        html += f'<img src="{{{{ url_for(\'static\', filename=\'{filename}\') }}}}" alt="{filename}"><br>'

                
    html += '<input type="submit" value="Back">'
    html += "</body></html>"
    
    return render_template_string(html)



@app.route("/go_home", methods=['POST'])
def go_home():
    return redirect("/")

if __name__ == '__main__':
    app.run(host='localhost', port=5000)
