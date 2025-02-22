import flask

bp = flask.Blueprint("api", __name__, url_prefix="/api/v1/")


@bp.route("/getPackageInfo", methods=["GET", "POST"])
def index():
    package_name = flask.request.values.get("package_name")
    if not package_name:
        return {"error": 1, "errmsg": "argument 'package_name' not set"}, 400
    return package_name
