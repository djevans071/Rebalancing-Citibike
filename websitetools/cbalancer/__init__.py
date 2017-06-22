from flask import Flask
app = Flask(__name__)

from cbalancer import views
