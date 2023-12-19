from flask import Flask, render_template, Response, request, redirect, url_for
from controller import map_consumer, table_consumer, publisher
from threading import Thread

app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def home():
    if request.method == 'GET':
        return render_template("Home.html")

    if request.method == 'POST':
        static_url = request.form["static_url"]
        realtime_url = request.form["realtime_url"]
        t1 = Thread(target=publisher, args=(static_url, realtime_url), daemon=True)
        t1.start()
        return redirect(url_for("map"))

@app.route('/map')
def map():
    return render_template("map.html")
            

@app.route('/stream')
def stream():
    def consume():
        try:
            map_consumer.subscribe(["Bus"])
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
            table_consumer.subscribe(["RealTime"])
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