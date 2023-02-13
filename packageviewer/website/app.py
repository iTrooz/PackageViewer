from flask import Flask

from packageviewer.website import bp_static


def create_app():
    app = Flask(__name__)
    
    app.register_blueprint(bp_static.bp)
    
    return app
