import hashlib
import json
import sqlite3
import time
import zipfile
from calendar import monthrange
from datetime import date, datetime, timezone
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "config.json"


def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for block in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def month_periods(start, end):
    current = date.fromisoformat(start)
    last = date.fromisoformat(end)
    while current <= last:
        month_end = date(current.year, current.month, monthrange(current.year, current.month)[1])
        yield current, min(month_end, last)
        current = month_end.fromordinal(month_end.toordinal() + 1)


def load_config():
    with CONFIG_PATH.open("r", encoding="utf-8") as file:
        config = json.load(file)
    date.fromisoformat(config["start_date"])
    date.fromisoformat(config["end_date"])
    return config


def connect_db(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    connection.execute(
        """CREATE TABLE IF NOT EXISTS runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            status TEXT NOT NULL
        )"""
    )
    connection.execute(
        """CREATE TABLE IF NOT EXISTS operations (
            operation_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            source TEXT NOT NULL,
            period TEXT NOT NULL,
            status TEXT NOT NULL,
            attempts INTEGER NOT NULL,
            items_count INTEGER,
            items_type TEXT,
            bytes_written INTEGER,
            checksum TEXT,
            file_path TEXT,
            source_url TEXT,
            error TEXT,
            created_at TEXT NOT NULL
        )"""
    )
    connection.execute(
        """CREATE TABLE IF NOT EXISTS partitions (
            source TEXT NOT NULL,
            period TEXT NOT NULL,
            checksum TEXT NOT NULL,
            file_path TEXT NOT NULL,
            PRIMARY KEY (source, period)
        )"""
    )
    connection.commit()
    return connection


def already_loaded(db, source, period, path):
    row = db.execute(
        "SELECT checksum, file_path FROM partitions WHERE source=? AND period=?",
        (source, period),
    ).fetchone()
    if not row:
        return False
    saved_path = Path(row[1])
    return saved_path.is_file() and sha256(saved_path) == row[0] and saved_path == path


def save_operation(db, run_id, source, period, status, attempts=0,
                   items_count=None, items_type=None, bytes_written=None,
                   checksum=None, file_path=None, source_url=None, error=None):
    db.execute(
        """INSERT INTO operations (
            run_id, source, period, status, attempts, items_count, items_type,
            bytes_written, checksum, file_path, source_url, error, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (run_id, source, period, status, attempts, items_count, items_type,
         bytes_written, checksum, file_path, source_url, error, now()),
    )
    db.commit()


def request_with_retry(method, url, **kwargs):
    last_error = None
    for attempt in range(1, 4):
        try:
            response = requests.request(method, url, timeout=90, **kwargs)
            if response.status_code == 429 or response.status_code >= 500:
                last_error = f"HTTP {response.status_code}"
                if attempt < 3:
                    time.sleep(attempt * 2)
                    continue
            return response, attempt
        except requests.RequestException as error:
            last_error = str(error)
            if attempt < 3:
                time.sleep(attempt * 2)
    raise RuntimeError(f"не удалось выполнить запрос после 3 попыток: {last_error}")


def atomic_save(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".partial")
    try:
        temporary.write_bytes(content)
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def download_eea(db, run_id, config, period_start, period_end):
    country = config["eea"]["country"]
    pollutant = config["eea"]["pollutant"]
    period = f"{period_start:%Y-%m}"
    path = ROOT / config["data_dir"] / "raw" / "eea" / f"{country}_{pollutant}_{period}.zip"
    if already_loaded(db, "eea", period, path):
        save_operation(db, run_id, "eea", period, "skipped", file_path=str(path))
        print(f"eea {period}: пропуск, файл уже загружен")
        return False

    url = config["eea"]["url"]
    body = {
        "countries": [country],
        "cities": [],
        "pollutants": [pollutant],
        "dataset": config["eea"]["dataset"],
        "source": "API",
        "method": "classic",
        "dateTimeStart": f"{period_start:%Y-%m-%d}T00:00:00Z",
        "dateTimeEnd": f"{period_end:%Y-%m-%d}T23:59:59Z",
        "aggregationType": "hour",
        "compress": True,
    }
    attempts = 0
    try:
        response, attempts = request_with_retry("POST", url, json=body)
        path.parent.mkdir(parents=True, exist_ok=True)
        if response.status_code != 200:
            raise RuntimeError(f"EEA вернул HTTP {response.status_code}; полный архив не получен")
        content = response.content
        temporary = path.with_suffix(path.suffix + ".check")
        temporary.write_bytes(content)
        try:
            if not zipfile.is_zipfile(temporary):
                raise ValueError("ответ EEA не является ZIP-архивом")
            with zipfile.ZipFile(temporary) as archive:
                if archive.testzip() is not None:
                    raise ValueError("в ZIP-архиве обнаружен повреждённый файл")
                archive_files = [name for name in archive.namelist() if not name.endswith("/")]
                parquet_files = [name for name in archive_files if name.lower().endswith(".parquet")]
                if not parquet_files:
                    archive_preview = ", ".join(archive_files[:5]) if archive_files else "архив пуст"
                    raise ValueError(f"в архиве EEA нет файлов Parquet; содержимое: {archive_preview}")
        finally:
            temporary.unlink(missing_ok=True)
        atomic_save(path, content)
        checksum = sha256(path)
        size = path.stat().st_size
        db.execute(
            "INSERT OR REPLACE INTO partitions (source, period, checksum, file_path) VALUES (?, ?, ?, ?)",
            ("eea", period, checksum, str(path)),
        )
        db.commit()
        save_operation(db, run_id, "eea", period, "success", attempts,
                       len(parquet_files), "файлов Parquet", size, checksum,
                       str(path), response.url)
        print(f"eea {period}: загружено, Parquet-файлов: {len(parquet_files)}, размер: {size} байт")
        return True
    except Exception as error:
        save_operation(db, run_id, "eea", period, "failed", attempts,
                       file_path=str(path), source_url=url, error=str(error))
        print(f"eea {period}: ошибка — {error}")
        return False


def download_weather(db, run_id, config, period_start, period_end):
    weather = config["open_meteo"]
    period = f"{period_start:%Y-%m}"
    coordinates = f"{weather['latitude']}_{weather['longitude']}"
    partition = f"{period}_{coordinates}"
    location = weather["location"]
    path = ROOT / config["data_dir"] / "raw" / "open_meteo" / f"weather_{location}_{partition}.json"
    if already_loaded(db, "open_meteo", partition, path):
        save_operation(db, run_id, "open_meteo", partition, "skipped", file_path=str(path))
        print(f"open-meteo {partition}: пропуск, файл уже загружен")
        return False

    params = {
        "latitude": weather["latitude"],
        "longitude": weather["longitude"],
        "start_date": period_start.isoformat(),
        "end_date": period_end.isoformat(),
        "hourly": ",".join(weather["hourly"]),
        "timezone": weather["timezone"],
        "wind_speed_unit": "ms",
    }
    url = weather["url"]
    attempts = 0
    try:
        response, attempts = request_with_retry("GET", url, params=params)
        if response.status_code != 200:
            raise RuntimeError(f"Open-Meteo вернул HTTP {response.status_code}")
        payload = response.json()
        hourly = payload.get("hourly", {})
        times = hourly.get("time")
        if not isinstance(times, list) or not times:
            raise ValueError("в ответе Open-Meteo отсутствует почасовой ряд")
        for variable in weather["hourly"]:
            values = hourly.get(variable)
            if not isinstance(values, list) or len(values) != len(times):
                raise ValueError(f"неверный или неполный массив {variable}")
        payload.pop("generationtime_ms", None)
        content = (json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n").encode("utf-8")
        atomic_save(path, content)
        checksum = sha256(path)
        size = path.stat().st_size
        db.execute(
            "INSERT OR REPLACE INTO partitions (source, period, checksum, file_path) VALUES (?, ?, ?, ?)",
            ("open_meteo", partition, checksum, str(path)),
        )
        db.commit()
        save_operation(db, run_id, "open_meteo", partition, "success", attempts,
                       len(times), "почасовых наблюдений", size, checksum,
                       str(path), response.url)
        print(f"open-meteo {partition}: загружено, наблюдений: {len(times)}, размер: {size} байт")
        return True
    except Exception as error:
        save_operation(db, run_id, "open_meteo", partition, "failed", attempts,
                       file_path=str(path), source_url=url, error=str(error))
        print(f"open-meteo {partition}: ошибка — {error}")
        return False


def main():
    config = load_config()
    data_dir = ROOT / config["data_dir"]
    db = connect_db(data_dir / "meta" / "load_journal.sqlite")
    started = now()
    cursor = db.execute("INSERT INTO runs (started_at, status) VALUES (?, ?)", (started, "running"))
    run_id = cursor.lastrowid
    db.commit()

    for start, end in month_periods(config["start_date"], config["end_date"]):
        download_eea(db, run_id, config, start, end)
        download_weather(db, run_id, config, start, end)

    failed = db.execute(
        "SELECT COUNT(*) FROM operations WHERE run_id=? AND status='failed'", (run_id,)
    ).fetchone()[0]
    completed = db.execute(
        "SELECT COUNT(*) FROM operations WHERE run_id=? AND status IN ('success', 'skipped')", (run_id,)
    ).fetchone()[0]
    if failed == 0:
        status = "success"
    elif completed > 0:
        status = "partial"
    else:
        status = "failed"
    db.execute("UPDATE runs SET finished_at=?, status=? WHERE run_id=?", (now(), status, run_id))
    db.commit()

    print("\nЖурнал загрузки:")
    rows = db.execute(
        "SELECT source, period, status, attempts, items_count, items_type, bytes_written, checksum, error "
        "FROM operations WHERE run_id=? ORDER BY operation_id",
        (run_id,),
    ).fetchall()
    for row in rows:
        print(" | ".join("" if value is None else str(value) for value in row))
    print(f"\nЗапуск {run_id}: {status}")
    print(f"Журнал SQLite: {data_dir / 'meta' / 'load_journal.sqlite'}")
    db.close()
    return 0 if status == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main())
