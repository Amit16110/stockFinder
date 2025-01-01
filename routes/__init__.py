from flask import Blueprint

# Define Blueprint
home_bp = Blueprint('home',__name__)
# dataBp = Blueprint('data',__name__)


# import routes
from .home import *