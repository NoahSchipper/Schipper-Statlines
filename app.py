from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS

from players import players_bp
from teams import teams_bp


app = Flask(__name__, static_folder="static")

CORS(app, resources={
    r"/*": {
        "origins": [
            "https://schipperstatlines.onrender.com",
            "https://website-a7a.pages.dev",
            "http://127.0.0.1:5501",
            "http://localhost:5501",
            "http://127.0.0.1:5500",
            "http://localhost:5500",
            "https://noahschipper.net",
        ],
    }
})

app.register_blueprint(players_bp)
app.register_blueprint(teams_bp)


@app.route("/")
def serve_index():
    return send_from_directory("static", "index.html")


@app.route("/health", methods=["GET"])
def health():
    return jsonify(status="ok"), 200


if __name__ == "__main__":
    import os

    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
