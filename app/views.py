from flask import Flask, render_template, Response
from controller import *


app = Flask(__name__)

@app.route('/')
def home():
    return render_template("Home.html")


@app.route('/map')
def map():
    return render_template("map.html")
            

@app.route('/stream')
def stream():
    def consume():
        try:
            consumer.subscribe(["bus_locations"])
            while 1:
                msg = consumer.poll(1.0)
                if msg is None:
                    print("Waiting")
                else:
                    yield f'data:{msg.value().decode("utf_8")}\n\n'
        finally:
            consumer.close()

    return Response(consume(), mimetype='text/event-stream')


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8000, debug=True)