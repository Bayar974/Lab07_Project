import logging
import os

from flask import Flask, jsonify, render_template, request
from flask_cors import CORS


from models.movie import Movie
#Лог
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)
#Апп
app = Flask(__name__,
            template_folder="../templates",
            static_folder="../static")
#CORS:PRODUCTION-д зөвхөн зөвшөөрөгдсөн домейн аас хандана
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS","*")
CORS(app, origins=ALLOWED_ORIGINS)
#Туслах функц
def error_response(message:str, code:int):
  """Нэгдсэн алдааны хариу."""
  return jsonify({"error": message, "status":code}), code

#Маршрутууд
@app.route("/")
def home():
  """Нүүр хуудас."""
  return render_template("index.html")

@app.route("/api/movies", methods=["GET"])
def get_movies():
  """Бүх киноны жагсаалт.

  Query params:
    genre - жанраар шүүх (?genre=Action)
    limit - хязгаар (?limit=10)
  """
  try:
    genre = request.args.get("genre")
    limit = request.args.get("limit", type=int)
    movies = Movie().get_all(genre=genre, limit=limit)
    return jsonify({"data": movies, "count": len(movies)}), 200
  except Exception as exc:
    log.exception("get_movies алдаа")
    return error_response(str(exc), 500)

@app.route("/api/movies/<int:movie_id>", methods=["GET"])
def get_movie(movie_id: int):
  """Нэг киноны дэлгэрэнгүй ."""
  try: 
    movie = Movie().get_by_id(movie_id)
    if movie:
      return jsonify({"data":movie}), 200
    return error_response("Кино олдсонгүй", 404)

  except Exception as exc:
    log.exception("get_movie алдаа - id=%s", movie_id)
    return error_response(str(exc), 500)

@app.route("/api/movies", methods=["POST"])
def create_movie():
  """Шинэ кино нэмэх."""
  body = request.get_json(silent=True)
  if not body:
    return error_response("JSON биш хүсэлт", 400)
  required = {"title", "release_year", "genre"}
  missing = required - body.keys()
  if missing:
    return error_response(f"Талбар дутуу:{','.join(missing)}", 400)
  try: 
    new_id = Movie().create(body)
    return jsonify({"data": {"id": new_id}, "message": "Амжилттай нэмлээ"}), 201
  except Exception as exc:
    log.exception("create_movie алдаа")
    return error_response(str(exc), 500)
@app.route("/admin/login")
def admin_login():
  return render_template("admin_login.html")

@app.route("/admin/panel")
def admin_panel():
  return render_template("admin_panel.html")

@app.route("/api/admin/login", methods=["POST"])
def admin_auth():
  body = request.get_json()
  if body.get("username") == "admin" and body.get("password") == "admin123":
    return jsonify({"token": "admin-token-123"}), 200
  return jsonify({"error": "Буруу нэвтрэх мэдээлэл"}), 401
@app.route("/api/movies/<int:movie_id>", methods=["PUT"])
def update_movie(movie_id):
  body = request.get_json()
  try:
    Movie().update(body, movie_id)
    return jsonify({"message": "Шинэчлэгдлээ"}), 200
  except Exception as exc:
    return error_response(str(exc), 500)    
@app.route("/api/movies/<int:movie_id>", methods=["DELETE"])
def delete_movie(movie_id):
  try: 
    Movie().delete(movie_id)
    return jsonify({"message": "Устгагдлаа"}), 200
  except Exception as exc:
    return error_response(str(exc), 500)
#404 / 500 глобал handler
@app.errorhandler(404)
def not_found(_):
  return error_response("Хуудас олдсонгүй", 404)

@app.errorhandler(500)
def server_error(_):
  return error_response("Серверийн алдаа", 500)

# Эхлүүлэх
if __name__ == "__main__":
  debug = os.getenv("FLASK_DEBUG", "false").lower() == "true"
  app.run(host="0.0.0.0", port=5000, debug=debug)
