from flask import Flask, render_template, Response
from controller import map_consumer, table_consumer


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
            map_consumer.subscribe(["bus_locations"])
            while 1:
                msg = map_consumer.poll(1.0)
                if msg is None:
                    # print("Waiting")
                    pass
                else:
                    yield f'data:{msg.value().decode("utf_8")}\n\n'
        except KeyboardInterrupt:
            return
        finally:
            map_consumer.close()

    return Response(consume(), mimetype='text/event-stream')


@app.route('/forecast')
def table():
    return render_template("forecast.html")

@app.route('/predict')
def predict():
    def consume():
        try:
            table_consumer.subscribe(["predictions"])
            while 1:
                msg = table_consumer.poll(1.0)
                if msg is None:
                    pass
                else:
                    yield f'data:{msg.value().decode("utf_8")}\n\n'
        except KeyboardInterrupt:
            return
        finally:
            table_consumer.close()

    return Response(consume(), mimetype='text/event-stream')