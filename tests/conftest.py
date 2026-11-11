import pytest

from WM_Central import db


@pytest.fixture(autouse=True)
def bd_temporal(tmp_path, monkeypatch):
    """Cada test usa su propia BD: así no se ensucia water.db con estaciones de prueba."""
    monkeypatch.setattr(db, "DB_PATH", str(tmp_path / "test.db"))
    db.crear_tablas()
