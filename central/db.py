"""
db.py: acceso a la base de datos SQLite de CENTRAL (obligatorio usar SQLite)
Responsable: Abde · Semana 2 · Tarjeta Trello: "SQLite: esquema + db.py + conectarlo a Central"
Lo usa: central/WM_Central.py

QUÉ TIENE QUE TENER
  - Fichero de BD configurable (por defecto "water.db"; en Railway, dentro del volumen persistente)
  - crear_tablas(): crea las tablas si no existen e inserta 2 o 3 operarios de prueba
        estaciones(id TEXT PK, ubicacion TEXT, estado TEXT)
        operarios(id TEXT PK, nombre TEXT)
        riegos(ws_id, operario_id, inicio, fin, volumen_total)   <- opcional
  - get_estaciones() -> lista de (id, ubicacion, estado)
  - guardar_estacion(id, ubicacion)   -> insertar o actualizar (INSERT OR REPLACE)
  - set_estado(id, estado)
  - operario_existe(id) -> bool
  - guardar_riego(...)                -> opcional

IMPORTANTE (hilos): Central es multihilo.
  sqlite3.connect(..., check_same_thread=False) + un threading.Lock() alrededor de cada consulta.

HECHO CUANDO: al reiniciar Central, las WS siguen en la BD y aparecen como DESCONECTADA
"""
