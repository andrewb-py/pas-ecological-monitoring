import hashlib
import json
import re
import sqlite3
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "processing.json"

EEA_REQUIRED = {
    "Samplingpoint", "Pollutant", "Start", "End", "Value", "Unit",
    "AggType", "Validity", "Verification", "ResultTime"
}
WEATHER_REQUIRED = {
    "time", "temperature_2m", "relative_humidity_2m", "precipitation",
    "pressure_msl", "wind_speed_10m", "wind_direction_10m"
}


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for block in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def atomic_write_parquet(table, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".partial")
    try:
        pq.write_table(table, temporary, compression="snappy")
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def connect_journal(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path)
    db.execute(
        """CREATE TABLE IF NOT EXISTS processing_runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            status TEXT NOT NULL,
            eea_rows_raw INTEGER,
            eea_rows_valid INTEGER,
            eea_rows_rejected INTEGER,
            duplicate_rows INTEGER,
            station_count INTEGER,
            weather_rows INTEGER,
            target_station_rows INTEGER,
            mart_rows INTEGER,
            error TEXT
        )"""
    )
    db.execute(
        """CREATE TABLE IF NOT EXISTS schema_versions (
            schema_id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_name TEXT NOT NULL,
            schema_hash TEXT NOT NULL,
            schema_definition TEXT NOT NULL,
            detected_at TEXT NOT NULL,
            UNIQUE(source_name, schema_hash)
        )"""
    )
    db.commit()
    return db


def register_schema(db, source_name, definition):
    definition_text = json.dumps(definition, ensure_ascii=False, sort_keys=True)
    schema_hash = hashlib.sha256(definition_text.encode("utf-8")).hexdigest()
    db.execute(
        """INSERT OR IGNORE INTO schema_versions
           (source_name, schema_hash, schema_definition, detected_at)
           VALUES (?, ?, ?, ?)""",
        (source_name, schema_hash, definition_text,
         datetime.now(timezone.utc).isoformat(timespec="seconds")),
    )
    db.commit()


def load_config():
    with CONFIG_PATH.open("r", encoding="utf-8") as file:
        return json.load(file)


def read_eea_archive(path):
    if not path.is_file():
        raise FileNotFoundError(f"не найден архив EEA: {path}")

    with zipfile.ZipFile(path) as archive:
        bad_file = archive.testzip()
        if bad_file is not None:
            raise ValueError(f"повреждённый файл внутри архива: {bad_file}")
        parquet_names = [
            name for name in archive.namelist()
            if name.lower().endswith(".parquet") and not name.endswith("/")
        ]
        if not parquet_names:
            raise ValueError("в архиве EEA не найден файл Parquet")

        tables = [
            pq.read_table(pa.BufferReader(archive.read(name)))
            for name in parquet_names
        ]

    if len(tables) == 1:
        table = tables[0]
    else:
        table = pa.concat_tables(tables, promote_options="permissive")

    actual = set(table.column_names)
    missing = sorted(EEA_REQUIRED - actual)
    if missing:
        raise ValueError("в схеме EEA отсутствуют обязательные поля: " + ", ".join(missing))

    schema_definition = [
        {"name": field.name, "type": str(field.type), "nullable": field.nullable}
        for field in table.schema
    ]
    return table, parquet_names, schema_definition


def read_weather(path):
    if not path.is_file():
        raise FileNotFoundError(f"не найден файл Open-Meteo: {path}")

    with path.open("r", encoding="utf-8") as file:
        payload = json.load(file)

    if "hourly" not in payload or not isinstance(payload["hourly"], dict):
        raise ValueError("в JSON Open-Meteo отсутствует объект hourly")

    hourly = payload["hourly"]
    missing = sorted(WEATHER_REQUIRED - set(hourly))
    if missing:
        raise ValueError("в погодных данных отсутствуют обязательные поля: " + ", ".join(missing))

    lengths = {key: len(hourly[key]) for key in WEATHER_REQUIRED}
    if len(set(lengths.values())) != 1:
        raise ValueError("массивы погодных показателей имеют разную длину")

    schema_definition = {
        "root_fields": sorted(payload.keys()),
        "hourly_fields": sorted(hourly.keys()),
        "hourly_units": payload.get("hourly_units", {}),
        "timezone": payload.get("timezone"),
    }
    return payload, schema_definition


def add_provenance(table, source_file, checksum, source_name):
    count = table.num_rows
    table = table.append_column("_source_system", pa.array([source_name] * count))
    table = table.append_column("_source_file", pa.array([source_file] * count))
    table = table.append_column("_source_sha256", pa.array([checksum] * count))
    return table


def station_id_from_samplingpoint(value):
    match = re.search(r"SPO_(PL[A-Z0-9]+)_", str(value))
    return match.group(1) if match else None


def create_weather_tables(payload, source_file, checksum):
    hourly = payload["hourly"]
    times = [datetime.fromisoformat(value) for value in hourly["time"]]

    bronze_columns = {
        "time": hourly["time"],
        "temperature_2m": hourly["temperature_2m"],
        "relative_humidity_2m": hourly["relative_humidity_2m"],
        "precipitation": hourly["precipitation"],
        "pressure_msl": hourly["pressure_msl"],
        "wind_speed_10m": hourly["wind_speed_10m"],
        "wind_direction_10m": hourly["wind_direction_10m"],
        "_source_system": ["Open-Meteo"] * len(times),
        "_source_file": [source_file] * len(times),
        "_source_sha256": [checksum] * len(times),
        "_timezone": [payload.get("timezone")] * len(times),
        "_response_latitude": [payload.get("latitude")] * len(times),
        "_response_longitude": [payload.get("longitude")] * len(times),
        "_hourly_units_json": [json.dumps(payload.get("hourly_units", {}), ensure_ascii=False, sort_keys=True)] * len(times),
    }
    bronze = pa.table(bronze_columns)

    silver_rows = []
    for index, timestamp in enumerate(times):
        row = {
            "timestamp": timestamp,
            "temperature_c": hourly["temperature_2m"][index],
            "relative_humidity_pct": hourly["relative_humidity_2m"][index],
            "precipitation_mm": hourly["precipitation"][index],
            "pressure_hpa": hourly["pressure_msl"][index],
            "wind_speed_ms": hourly["wind_speed_10m"][index],
            "wind_direction_deg": hourly["wind_direction_10m"][index],
        }
        for key, value in row.items():
            if value is None:
                raise ValueError(f"в Open-Meteo есть пропуск в поле {key}")
        if not 0 <= row["relative_humidity_pct"] <= 100:
            raise ValueError("влажность выходит за допустимый диапазон 0–100 %")
        if row["precipitation_mm"] < 0 or row["wind_speed_ms"] < 0:
            raise ValueError("осадки и скорость ветра не могут быть отрицательными")
        silver_rows.append(row)

    if len({row["timestamp"] for row in silver_rows}) != len(silver_rows):
        raise ValueError("в погодных данных обнаружены повторяющиеся временные метки")

    silver_schema = pa.schema([
        ("timestamp", pa.timestamp("us")),
        ("temperature_c", pa.float64()),
        ("relative_humidity_pct", pa.float64()),
        ("precipitation_mm", pa.float64()),
        ("pressure_hpa", pa.float64()),
        ("wind_speed_ms", pa.float64()),
        ("wind_direction_deg", pa.float64()),
    ])
    silver = pa.Table.from_pylist(silver_rows, schema=silver_schema)
    return bronze, silver


def process_eea(table, source_file, checksum):
    source = table.to_pydict()
    bronze = add_provenance(table, source_file, checksum, "EEA")

    accepted = {}
    rejected = []
    duplicate_count = 0

    for index in range(table.num_rows):
        sampling_point = source["Samplingpoint"][index]
        station_id = station_id_from_samplingpoint(sampling_point)
        pollutant = source["Pollutant"][index]
        start = source["Start"][index]
        end = source["End"][index]
        value = source["Value"][index]
        unit = source["Unit"][index]
        aggregation = source["AggType"][index]
        validity = source["Validity"][index]
        verification = source["Verification"][index]
        result_time = source["ResultTime"][index]

        reason = None
        if station_id is None:
            reason = "не удалось определить код станции"
        elif pollutant != 5:
            reason = "измерение относится не к PM10"
        elif unit != "ug.m-3":
            reason = "неожиданная единица измерения"
        elif aggregation != "hour":
            reason = "измерение не является почасовым"
        elif start is None or end is None or end <= start:
            reason = "некорректный временной интервал"
        elif validity not in (1, 3):
            reason = "измерение помечено как недействительное"
        elif verification != 1:
            reason = "измерение не имеет статуса verified"
        elif value is None or float(value) == -999:
            reason = "отсутствует числовое значение"
        elif float(value) < 0:
            reason = "отрицательное значение концентрации"

        value_float = None if value is None else float(value)
        if reason is not None:
            rejected.append({
                "sampling_point": sampling_point,
                "station_id": station_id,
                "measurement_start": start,
                "value": value_float,
                "validity_flag": validity,
                "verification_flag": verification,
                "reject_reason": reason,
            })
            continue

        key = (sampling_point, start, pollutant)
        record = {
            "station_id": station_id,
            "sampling_point": sampling_point,
            "pollutant_code": pollutant,
            "measurement_start": start,
            "measurement_end": end,
            "pm10_ug_m3": value_float,
            "validity_flag": validity,
            "verification_flag": verification,
            "result_time": result_time,
            "unit": "µg/m³",
        }

        if key in accepted:
            duplicate_count += 1
            old_result_time = accepted[key]["result_time"]
            if result_time is not None and (old_result_time is None or result_time > old_result_time):
                accepted[key] = record
        else:
            accepted[key] = record

    silver_rows = sorted(
        accepted.values(),
        key=lambda row: (row["station_id"], row["measurement_start"])
    )

    silver_schema = pa.schema([
        ("station_id", pa.string()),
        ("sampling_point", pa.string()),
        ("pollutant_code", pa.int32()),
        ("measurement_start", pa.timestamp("us")),
        ("measurement_end", pa.timestamp("us")),
        ("pm10_ug_m3", pa.float64()),
        ("validity_flag", pa.int32()),
        ("verification_flag", pa.int32()),
        ("result_time", pa.timestamp("us")),
        ("unit", pa.string()),
    ])
    reject_schema = pa.schema([
        ("sampling_point", pa.string()),
        ("station_id", pa.string()),
        ("measurement_start", pa.timestamp("us")),
        ("value", pa.float64()),
        ("validity_flag", pa.int32()),
        ("verification_flag", pa.int32()),
        ("reject_reason", pa.string()),
    ])
    silver = pa.Table.from_pylist(silver_rows, schema=silver_schema)
    rejects = pa.Table.from_pylist(rejected, schema=reject_schema)
    return bronze, silver, rejects, duplicate_count


def create_dimensions_and_mart(silver_pm10, silver_weather, target):
    pm10_rows = silver_pm10.to_pylist()
    weather_rows = silver_weather.to_pylist()

    sampling_points = {}
    for row in pm10_rows:
        sampling_points[row["station_id"]] = row["sampling_point"]

    station_rows = []
    for station_id in sorted(sampling_points):
        is_target = station_id == target["station_id"]
        station_rows.append({
            "station_id": station_id,
            "sampling_point": sampling_points[station_id],
            "city": target["city"] if is_target else None,
            "station_name": target["station_name"] if is_target else None,
            "latitude": target["latitude"] if is_target else None,
            "longitude": target["longitude"] if is_target else None,
            "station_type": target["station_type"] if is_target else None,
            "station_area": target["station_area"] if is_target else None,
            "metadata_source": target["metadata_source"] if is_target else None,
        })

    station_schema = pa.schema([
        ("station_id", pa.string()),
        ("sampling_point", pa.string()),
        ("city", pa.string()),
        ("station_name", pa.string()),
        ("latitude", pa.float64()),
        ("longitude", pa.float64()),
        ("station_type", pa.string()),
        ("station_area", pa.string()),
        ("metadata_source", pa.string()),
    ])
    dim_station = pa.Table.from_pylist(station_rows, schema=station_schema)

    target_pm10 = [row for row in pm10_rows if row["station_id"] == target["station_id"]]
    if not target_pm10:
        raise ValueError(f"в EEA не найдены данные для станции {target['station_id']}")

    weather_by_time = {row["timestamp"]: row for row in weather_rows}
    mart_rows = []
    for row in target_pm10:
        weather = weather_by_time.get(row["measurement_start"])
        mart_rows.append({
            "station_id": row["station_id"],
            "station_name": target["station_name"],
            "city": target["city"],
            "latitude": target["latitude"],
            "longitude": target["longitude"],
            "timestamp": row["measurement_start"],
            "period_end": row["measurement_end"],
            "pm10_ug_m3": row["pm10_ug_m3"],
            "validity_flag": row["validity_flag"],
            "verification_flag": row["verification_flag"],
            "temperature_c": None if weather is None else weather["temperature_c"],
            "relative_humidity_pct": None if weather is None else weather["relative_humidity_pct"],
            "precipitation_mm": None if weather is None else weather["precipitation_mm"],
            "pressure_hpa": None if weather is None else weather["pressure_hpa"],
            "wind_speed_ms": None if weather is None else weather["wind_speed_ms"],
            "wind_direction_deg": None if weather is None else weather["wind_direction_deg"],
            "weather_matched": weather is not None,
        })

    mart_schema = pa.schema([
        ("station_id", pa.string()),
        ("station_name", pa.string()),
        ("city", pa.string()),
        ("latitude", pa.float64()),
        ("longitude", pa.float64()),
        ("timestamp", pa.timestamp("us")),
        ("period_end", pa.timestamp("us")),
        ("pm10_ug_m3", pa.float64()),
        ("validity_flag", pa.int32()),
        ("verification_flag", pa.int32()),
        ("temperature_c", pa.float64()),
        ("relative_humidity_pct", pa.float64()),
        ("precipitation_mm", pa.float64()),
        ("pressure_hpa", pa.float64()),
        ("wind_speed_ms", pa.float64()),
        ("wind_direction_deg", pa.float64()),
        ("weather_matched", pa.bool_()),
    ])
    mart = pa.Table.from_pylist(mart_rows, schema=mart_schema)

    time_rows = []
    for timestamp in sorted({row["timestamp"] for row in mart_rows}):
        time_rows.append({
            "timestamp": timestamp,
            "calendar_date": timestamp.date(),
            "hour": timestamp.hour,
            "day_of_week": timestamp.weekday(),
            "month": timestamp.month,
            "year": timestamp.year,
        })
    time_schema = pa.schema([
        ("timestamp", pa.timestamp("us")),
        ("calendar_date", pa.date32()),
        ("hour", pa.int8()),
        ("day_of_week", pa.int8()),
        ("month", pa.int8()),
        ("year", pa.int16()),
    ])
    dim_time = pa.Table.from_pylist(time_rows, schema=time_schema)

    matched = sum(1 for row in mart_rows if row["weather_matched"])
    return dim_station, dim_time, mart, len(target_pm10), matched


def main():
    config = load_config()
    data_dir = ROOT / config["data_dir"]
    input_eea = ROOT / config["input"]["eea_zip"]
    input_weather = ROOT / config["input"]["weather_json"]
    target = config["target_station"]
    db = connect_journal(data_dir / "meta" / "processing.sqlite")

    started = datetime.now(timezone.utc).isoformat(timespec="seconds")
    cursor = db.execute(
        "INSERT INTO processing_runs (started_at, status) VALUES (?, ?)",
        (started, "running"),
    )
    run_id = cursor.lastrowid
    db.commit()

    try:
        eea_table, parquet_names, eea_schema = read_eea_archive(input_eea)
        weather_payload, weather_schema = read_weather(input_weather)
        register_schema(db, "EEA", eea_schema)
        register_schema(db, "Open-Meteo", weather_schema)

        eea_checksum = sha256_file(input_eea)
        weather_checksum = sha256_file(input_weather)

        bronze_eea, silver_pm10, rejects, duplicate_count = process_eea(
            eea_table, input_eea.name, eea_checksum
        )
        bronze_weather, silver_weather = create_weather_tables(
            weather_payload, input_weather.name, weather_checksum
        )
        dim_station, dim_time, mart, target_rows, weather_matches = create_dimensions_and_mart(
            silver_pm10, silver_weather, target
        )

        atomic_write_parquet(bronze_eea, data_dir / "bronze" / "eea_pm10.parquet")
        atomic_write_parquet(bronze_weather, data_dir / "bronze" / "open_meteo_weather.parquet")
        atomic_write_parquet(silver_pm10, data_dir / "silver" / "pm10_measurements.parquet")
        atomic_write_parquet(rejects, data_dir / "silver" / "pm10_rejected.parquet")
        atomic_write_parquet(silver_weather, data_dir / "silver" / "weather_hourly.parquet")
        atomic_write_parquet(dim_station, data_dir / "gold" / "dim_station.parquet")
        atomic_write_parquet(dim_time, data_dir / "gold" / "dim_time.parquet")
        atomic_write_parquet(mart, data_dir / "gold" / "mart_pm10_weather.parquet")

        result = {
            "eea_rows_raw": eea_table.num_rows,
            "eea_rows_valid": silver_pm10.num_rows,
            "eea_rows_rejected": rejects.num_rows,
            "duplicate_rows": duplicate_count,
            "station_count": dim_station.num_rows,
            "weather_rows": silver_weather.num_rows,
            "target_station_rows": target_rows,
            "mart_rows": mart.num_rows,
            "weather_matches": weather_matches,
        }
        finished = datetime.now(timezone.utc).isoformat(timespec="seconds")
        db.execute(
            """UPDATE processing_runs SET finished_at=?, status='success',
               eea_rows_raw=?, eea_rows_valid=?, eea_rows_rejected=?, duplicate_rows=?,
               station_count=?, weather_rows=?, target_station_rows=?, mart_rows=?
               WHERE run_id=?""",
            (finished, result["eea_rows_raw"], result["eea_rows_valid"],
             result["eea_rows_rejected"], result["duplicate_rows"],
             result["station_count"], result["weather_rows"],
             result["target_station_rows"], result["mart_rows"], run_id),
        )
        db.commit()

        print("Обработка завершена успешно.")
        print(f"EEA: исходных строк — {result['eea_rows_raw']}")
        print(f"Silver PM10: принято — {result['eea_rows_valid']}, отклонено — {result['eea_rows_rejected']}")
        print(f"Дубликатов удалено: {result['duplicate_rows']}")
        print(f"Станций в справочнике: {result['station_count']}")
        print(f"Open-Meteo: почасовых наблюдений — {result['weather_rows']}")
        print(f"Станция {target['station_id']}: измерений — {result['target_station_rows']}")
        print(f"Gold-витрина: строк — {result['mart_rows']}, совпадений с погодой — {result['weather_matches']}")
        print(f"Журнал обработки: {data_dir / 'meta' / 'processing.sqlite'}")
        print("Созданы слои: data/bronze, data/silver, data/gold")

    except Exception as error:
        finished = datetime.now(timezone.utc).isoformat(timespec="seconds")
        db.execute(
            "UPDATE processing_runs SET finished_at=?, status='failed', error=? WHERE run_id=?",
            (finished, str(error), run_id),
        )
        db.commit()
        print(f"Ошибка обработки: {error}")
        raise
    finally:
        db.close()


if __name__ == "__main__":
    main()
