import logging
import os
from contextlib import contextmanager
from typing import Optional
import mysql.connector
from mysql.connector import Error

log = logging.getLogger(__name__)

#Тохиргоо
DB_CONFIG = {
  "host": os.getenv("DB_HOST", "localhost"),
  "port": 3307,
  "user": os.getenv("DB_USER", "root"),
  "password": os.getenv("DB_PASSWORD", "root"),
  "database": os.getenv("DB_NAME", "netflix_db"),
  "charset": "utf8mb4",
}
class Movie:
#Холболт
  @contextmanager
  def _get_cursor(self):
    """Cursor үүсгэж, дуусмагц автоматаар хаадаг context manager."""
    conn = None
    try:
      conn = mysql.connector.connect(**DB_CONFIG)
      cursor = conn.cursor(dictionary=True)
      yield cursor, conn
    except Error:
      log.exception("DB холболтын алдаа")
      raise
    finally:
      if conn and conn.is_connected():
        conn.close()
#Унших
  def get_all(self, genre: Optional[str] = None, limit: Optional[int] = None) -> list:
    """Бүх кино - жанраар шүүх, тоог хязгаарлах боломжтой."""
    try: 
      with self._get_cursor() as (cursor, _):
        sql = "SELECT * FROM movies"
        params = []
        if genre:
          sql += " WHERE genre = %s"
          params.append(genre)
        
        sql += " ORDER BY release_year DESC"
        if limit:
          sql += " LIMIT %s"
          params.append(limit)
        
        cursor.execute(sql, params)
        return cursor.fetchall()
    except Error:
      return []
  
  def get_by_id(self, movie_id: int) -> Optional[dict]:
    """ID-аар нэг кино татах."""
    try: 
      with self._get_cursor() as (cursor, _):
        cursor.execute("SELECT * FROM movies WHERE id = %s", (movie_id,))
        return cursor.fetchone()
    except Error:
      return None

#Бичих
  def create(self, data: dict) -> int:
    """Шинэ кино нэмэх - шинэ ID -г буцаана."""
    sql = """
      INSERT INTO movies (title, description, release_year, genre, image_url)
      VALUES (%(title)s, %(description)s, %(release_year)s, %(genre)s, %(image_url)s)
    """
    try: 
      with self._get_cursor() as (cursor, conn):
        cursor.execute(sql, data)
        conn.commit()
        return cursor.lastrowid
    except Error:
      log.exception("create алдаа")
      raise
  
  def delete(self, movie_id: int) -> bool:
    """ID-аар кино устгах - амжилттай бол True."""
    try:
      with self._get_cursor() as (cursor, conn):
        cursor.execute("DELETE FROM movies WHERE id = %s", (movie_id,))
        conn.commit()
        return cursor.rowcount > 0
   
    except Error:
      log.exception("delete алдаа - id=%s", movie_id)
      return False
  def update(self, data:dict, movie_id:int) -> bool:
    sql = """
      UPDATE movies SET
        title = %(title)s,
        description = %(description)s,
        release_year = %(release_year)s,
        genre = %(genre)s,
        image_url = %(image_url)s
      WHERE id = %(id)s
    """
    data['id'] = movie_id
    try:
      with self._get_cursor() as (cursor, conn):
        cursor.execute(sql, data)
        conn.commit()
        return cursor.rowcount > 0
    except Error:
      log.exception("update алдаа -id=%s", movie_id)
      raise
