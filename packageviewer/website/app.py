from flask import Flask

from packageviewer.website import bp_static, bp_api


def create_app():
    app = Flask(__name__)
    
    app.register_blueprint(bp_static.bp)
    app.register_blueprint(bp_api.bp)
    
    return app
