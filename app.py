from flask import Flask, render_template, request, redirect, url_for, flash

app = Flask(__name__)

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/search')
def search(date, time):
    pass

if __name__ == '__main__':
    app.run(debug=True)
