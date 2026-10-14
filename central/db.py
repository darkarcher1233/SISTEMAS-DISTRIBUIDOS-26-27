"""Acceso a la base de datos SQLite de Central."""

import os
import sqlite3 #DB
import threading

DB_PATH = os.environ.get("WATER_DB", "water.db")
DB_LOCK = threading.Lock()


def _connect():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def crear_tablas():
    """Crea las tablas necesarias y rellena operarios de prueba."""
    with DB_LOCK:
        conn = _connect()
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS estaciones (
                    id TEXT PRIMARY KEY,
                    ubicacion TEXT,
                    estado TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS operarios (
                    id TEXT PRIMARY KEY,
                    nombre TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS riegos (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ws_id TEXT,
                    operario_id TEXT,
                    inicio TEXT,
                    fin TEXT,
                    volumen_total REAL
                )
                """
            )
            operarios = [
                ("fo1", "Operario 1"),
                ("fo2", "Operario 2"),
                ("fo3", "Operario 3"),
            ]
            conn.executemany(
                "INSERT OR IGNORE INTO operarios(id, nombre) VALUES (?, ?)",
                operarios,
            )
            conn.commit()
        finally:
            conn.close()


def _ensure_tables():
    try:
        crear_tablas()
    except sqlite3.Error:
        pass


def get_estaciones():
    """Devuelve la lista de estaciones guardadas en la BD."""
    _ensure_tables()
    with DB_LOCK:
        conn = _connect()
        try:
            filas = conn.execute(
                "SELECT id, ubicacion, estado FROM estaciones ORDER BY id"
            ).fetchall()
            return [(fila["id"], fila["ubicacion"], fila["estado"]) for fila in filas]
        finally:
            conn.close()


def guardar_estacion(id_estacion, ubicacion):
    """Inserta o actualiza una estación."""
    _ensure_tables()
    with DB_LOCK:
        conn = _connect()
        try:
            conn.execute(
                "INSERT OR REPLACE INTO estaciones(id, ubicacion, estado) VALUES (?, ?, COALESCE((SELECT estado FROM estaciones WHERE id = ?), 'DESCONECTADA'))",
                (id_estacion, ubicacion, id_estacion),
            )
            conn.commit()
        finally:
            conn.close()


def set_estado(id_estacion, estado):
    """Actualiza el estado de una estación existente."""
    _ensure_tables()
    with DB_LOCK:
        conn = _connect()
        try:
            conn.execute(
                "INSERT OR REPLACE INTO estaciones(id, ubicacion, estado) VALUES (?, COALESCE((SELECT ubicacion FROM estaciones WHERE id = ?), ''), ?)",
                (id_estacion, id_estacion, estado),
            )
            conn.commit()
        finally:
            conn.close()


def operario_existe(id_operario):
    """Devuelve True si el operario existe en la BD."""
    with DB_LOCK:
        conn = _connect()
        try:
            fila = conn.execute(
                "SELECT 1 FROM operarios WHERE id = ?",
                (id_operario,),
            ).fetchone()
            return fila is not None
        finally:
            conn.close()


def guardar_riego(ws_id, operario_id, inicio, fin, volumen_total):
    """Guarda el resumen final de un riego."""
    with DB_LOCK:
        conn = _connect()
        try:
            conn.execute(
                "INSERT INTO riegos(ws_id, operario_id, inicio, fin, volumen_total) VALUES (?, ?, ?, ?, ?)",
                (ws_id, operario_id, inicio, fin, volumen_total),
            )
            conn.commit()
        finally:
            conn.close()

