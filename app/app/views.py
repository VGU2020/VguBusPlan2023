from flask import Blueprint, render_template, render_template_string
from .controllers import renderMap

views = Blueprint('views', __name__)

@views.route('/')
def home():
    return render_template("home.html")

@views.route("/map")
def map():
    html, map = renderMap()

    return render_template_string(html, map=map)

