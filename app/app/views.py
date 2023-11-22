from flask import Blueprint, render_template
from .controllers import renderMap

views = Blueprint('views', __name__)


@views.route('/')
def home():
    return render_template("home.html")


@views.route("/map")
def map():
    html_tag = renderMap()
    return render_template("map.html", map=html_tag)


