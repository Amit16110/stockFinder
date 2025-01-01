from flask import Flask
from routes import home_bp
from config import Config
from model.database import db
from fetcher.fetcher import callFetecher,singleFetecher

app = Flask(__name__)
app.config.from_object(Config)


# Initialize the database
db.init_app(app)

# This active when the any request is trigerred 
# @app.before_first_request
# def create_tables():
#     db.create_all()

# Initialize wehn the server start
with app.app_context():
    db.create_all()
    callFetecher()
# Register blueprints
app.register_blueprint(home_bp)

if __name__ == '__main__':
    app.run(host="0.0.0.0",port=8080,debug=True)
