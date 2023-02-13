from flask import Blueprint


bp = Blueprint('static', __name__)

@bp.route("/")
def index():
    return "hey"