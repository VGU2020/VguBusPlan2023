# from flask_mongoengine import MongoEngine
from flask import Flask

# App and it's configuration
app = Flask(__name__, template_folder="templates")
# app.config['SECRET_KEY'] = "anhKhaikute"


# Views structure
from .views import views
app.register_blueprint(views, url_prefix='/')


def create_app():
    return app

