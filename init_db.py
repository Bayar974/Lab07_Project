import mysql.connector
from mysql.connector import Error
import logging
import sys
from datetime import datetime
TMDB_API_KEY = "30779d5c314b58918224662a4555de45"
# ── Logging ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("db_init.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ── DB тохиргоо ───────────────────────────────────────────────
DB_CONFIG = {
    "host":      "localhost",
    "port":      3307,
    "user":      "root",
    "password":  "root",
    "charset":   "utf8mb4",
    "autocommit": False,
}
DB_NAME = "netflix_db"

# ── Кино өгөгдөл + TMDB poster URL ───────────────────────────
MOVIES = [
    # ── Oscar шагналт ──────────────────────────────────────────
    ("Oppenheimer",             "Атомын бөмбөгийг бүтээсэн J. Robert Oppenheimer-ийн эпик биографик кино.", 2023, "Drama",     "https://image.tmdb.org/t/p/w500/8Gxv8gSFCU0XGDykEGv7zR1n2ua.jpg"),
    ("Parasite",                "Нийгмийн тэгш бус байдлыг харуулсан Солонгос кино.",                       2019, "Thriller",  "https://image.tmdb.org/t/p/w500/7IiTTgloJzvGI1TAYymCfbfl3vT.jpg"),
    ("Poor Things",             "Нас барсан эмэгтэйг амилуулсан эрдэмтний тухай хачирхалтай тууж.",         2023, "Drama",     "https://image.tmdb.org/t/p/w500/kCGlIMHnOm8JPXNbwithin5kwFAq8.jpg"),
    ("The Shawshank Redemption","Найрамдал ба эрх чөлөөний тухай мөнхийн сонгодог.",                        1994, "Drama",     "https://image.tmdb.org/t/p/w500/q6y0Go1tsGEsmtFryDOJo3dEmqu.jpg"),
    # ── Sci-Fi / Action / Thriller ─────────────────────────────
    ("Interstellar",            "Сансрын аялал тухай хамгийн гайхалтай кино.",                              2014, "Sci-Fi",    "https://image.tmdb.org/t/p/w500/gEU2QniE6E77NI6lZtvY9fOUGlL.jpg"),
    ("The Dark Knight",         "Батман ба Жокерийн эпик тулаан.",                                          2008, "Action",    "https://image.tmdb.org/t/p/w500/qJ2tW6WMUDux911r6m7haRef0WH.jpg"),
    ("Inception",               "Зүүдний давхарга дунд нуугдсан оюун сэтгэлийн шинжилгээч.",               2010, "Thriller",  "https://image.tmdb.org/t/p/w500/oYuLEt3zVCKq57qu2F8dT7NIa6f.jpg"),
    # ── Anime ──────────────────────────────────────────────────
    ("Spirited Away",           "Хүүхэд охин сүнснүүдийн ертөнцөд орж аав ээжээ аварна.",                  2001, "Anime",     "https://image.tmdb.org/t/p/w500/39wmItIWsg5sZMyRUHLkWBcuVCM.jpg"),
    ("Your Name",               "Хоёр залуу нойрны үед биеийг нь солилцон хувирдаг байна.",                2016, "Anime",     "https://image.tmdb.org/t/p/w500/q719jXXEzOoYaps6babgKnONONX.jpg"),
    ("Demon Slayer: Mugen Train","Танжиро ба найзууд нь галт тэргэн дэх чөтгөртэй тулалдана.",             2020, "Anime",     "https://image.tmdb.org/t/p/w500/h8Rb9gBr48ODIwYZ4RKDG3ecQCT.jpg"),
    ("Princess Mononoke",       "Хүн ба байгалийн хоорондох эртний тулаан.",                               1997, "Anime",     "https://image.tmdb.org/t/p/w500/dS8PBWxdKNiIoaFBMEJ5GRYjpMf.jpg"),
    # ── Disney / Pixar ─────────────────────────────────────────
    ("The Lion King",           "Хаан арслангийн хүүгийн залгамжилгааны тухай мөнхийн домог.",             1994, "Animation", "https://image.tmdb.org/t/p/w500/sKCr78MXSuC3wAA5PbKxBTcXOFH.jpg"),
    ("Coco",                    "Хөгжимчин хүүхэд нас барагчдын ертөнцөд орон гэр бүлийнхээ нууцыг тайлна.",2017,"Animation","https://image.tmdb.org/t/p/w500/gGEsBPAijhVUFoiNpgZXqRVWJt2.jpg"),
    ("Encanto",                 "Магадлал бүхий Мадригал гэр бүлд ганц л хүү тусгай чадваргүй.",           2021, "Animation", "https://image.tmdb.org/t/p/w500/4j0PNHkMr5ax3IA8tjtxcmPU3QT.jpg"),
    ("Soul",                    "Хөгжмийн багш оюун сүнсний ертөнцөд орж амьдралын учир утгыг нээнэ.",     2020, "Animation", "https://image.tmdb.org/t/p/w500/hm58Jw4Lw8OIeECIq5Wzqxm5p9.jpg"),
]

# ── SQL ───────────────────────────────────────────────────────
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS movies (
    id           INT AUTO_INCREMENT PRIMARY KEY,
    title        VARCHAR(255) NOT NULL,
    description  TEXT,
    release_year SMALLINT CHECK (release_year BETWEEN 1888 AND 2100),
    genre        VARCHAR(100),
    image_url    VARCHAR(500),
    created_at   DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""
INSERT_SQL = """
    INSERT INTO movies (title, description, release_year, genre, image_url)
    VALUES (%s, %s, %s, %s, %s)
"""

# ── Функцүүд ──────────────────────────────────────────────────
def get_connection(database=None):
    cfg = {**DB_CONFIG}
    if database:
        cfg["database"] = database
    return mysql.connector.connect(**cfg)

def setup_database(cursor):
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME} CHARACTER SET utf8mb4")
    log.info("Database '%s' бэлэн.", DB_NAME)
    cursor.execute(f"USE {DB_NAME}")
    cursor.execute(CREATE_TABLE_SQL)
    log.info("Хүснэгт 'movies' бэлэн.")

def seed_movies(cursor, conn):
    cursor.execute("DELETE FROM movies")
    cursor.executemany(INSERT_SQL, MOVIES)
    conn.commit()
    log.info("%d кино амжилттай оруулагдлаа.", cursor.rowcount)
    return cursor.rowcount

def verify(cursor):
    cursor.execute(
        "SELECT id, title, release_year, genre, image_url FROM movies ORDER BY release_year"
    )
    rows = cursor.fetchall()
    log.info("── Шалгалт ─────────────────────────────────────────")
    for r in rows:
        poster = "✅ байна" if r[4] else "❌ байхгүй"
        log.info("  [%2d] %-35s %d  %-12s  %s", r[0], r[1], r[2], r[3], poster)
    log.info("────────────────────────────────────────────────────")

def main():
    start = datetime.now()
    log.info("Netflix DB эхлүүлэлт эхэллээ...")
    conn = cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        setup_database(cursor)
        count = seed_movies(cursor, conn)
        verify(cursor)
        elapsed = (datetime.now() - start).total_seconds()
        log.info("Дууслаа — %d кино, %.2f секунд.", count, elapsed)
    except Error as db_err:
        log.error("MySQL алдаа: %s", db_err)
        if conn:
            conn.rollback()
        sys.exit(1)
    except Exception as err:
        log.error("Гэнэтийн алдаа: %s", err)
        if conn:
            conn.rollback()
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()
            log.info("Холболт хаагдлаа.")

if __name__ == "__main__":
    main()