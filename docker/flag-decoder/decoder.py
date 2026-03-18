import base64
import logging
import os
import re
import time
from urllib.parse import unquote

import psycopg2
import psycopg2.extras

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [flag-decoder] %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

TIMESCALE = os.environ["TIMESCALE"]
FLAG_REGEX = os.environ["FLAG_REGEX"]
INTERVAL = int(os.environ.get("INTERVAL", "30"))

PROCESSED_TAG = "flag-out-encoded"
SKIP_TAGS = {"flag-out", PROCESSED_TAG}


def decode_variants(raw: bytes) -> list:
    results = []
    text = raw.decode("utf-8", errors="replace")

    url_decoded = unquote(text)
    if url_decoded != text:
        results.append(("url", url_decoded))

    for match in re.finditer(r"[A-Za-z0-9+/]{20,}={0,2}", text):
        candidate = match.group(0)
        padding = (4 - len(candidate) % 4) % 4
        try:
            decoded = base64.b64decode(candidate + "=" * padding)
            decoded_str = decoded.decode("utf-8", errors="replace")
            results.append(("base64", decoded_str))
        except Exception:
            pass

    for match in re.finditer(r"(?:[0-9a-fA-F]{2}){8,}", text):
        candidate = match.group(0)
        try:
            decoded = bytes.fromhex(candidate)
            decoded_str = decoded.decode("utf-8", errors="replace")
            results.append(("hex", decoded_str))
        except Exception:
            pass

    return results


def find_flags(pattern, variants: list) -> list:
    found = []
    for encoding, text in variants:
        for flag in pattern.findall(text):
            found.append((encoding, flag))
    return found


def check_schema(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'flows'
            ORDER BY ordinal_position
        """)
        cols = {r[0] for r in cur.fetchall()}
        if not cols:
            raise SystemExit("Таблиця 'flows' не знайдена — TimescaleDB ще не ініціалізовано?")
        log.info("Колонки flows: %s", sorted(cols))

        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_name = 'flow_data'
        """)
        if not cur.fetchone():
            raise SystemExit("Таблиця 'flow_data' не знайдена.")

        has_flags_col = "flags" in cols
        if not has_flags_col:
            log.warning(
                "Колонка 'flags' відсутня у flows — "
                "знайдені прапори зберігатимуться лише у тегах."
            )
        return has_flags_col


def process_batch(conn, pattern, has_flags_col: bool) -> int:
    detected = 0
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("""
            SELECT id, tags
            FROM flows
            WHERE time > NOW() - INTERVAL '24 hours'
              AND NOT tags && %s::text[]
            LIMIT 500
        """, ([*SKIP_TAGS],))
        rows = cur.fetchall()

        if not rows:
            return 0

        for row in rows:
            flow_id = row["id"]
            tags = list(row["tags"] or [])

            cur.execute("""
                SELECT data FROM flow_data
                WHERE flow_id = %s AND from_client = false
                ORDER BY id
            """, (flow_id,))
            data_rows = cur.fetchall()

            if not data_rows:
                _mark_processed(cur, flow_id, tags)
                continue

            raw = b"".join(bytes(r["data"]) for r in data_rows)
            variants = decode_variants(raw)
            flags_found = find_flags(pattern, variants)

            if flags_found:
                detected += 1
                encodings = sorted({enc for enc, _ in flags_found})
                flag_values = [f for _, f in flags_found]
                log.info(
                    "flow %s: знайдено %d прапор(ів) через %s: %s",
                    flow_id,
                    len(flag_values),
                    ", ".join(encodings),
                    flag_values[:3],
                )
                new_tags = tags + [PROCESSED_TAG] + [f"encoding:{e}" for e in encodings]
                if has_flags_col:
                    cur.execute("""
                        UPDATE flows
                        SET tags = %s,
                            flags = COALESCE(flags, '{}') || %s
                        WHERE id = %s
                    """, (new_tags, flag_values, flow_id))
                else:
                    cur.execute(
                        "UPDATE flows SET tags = %s WHERE id = %s",
                        (new_tags, flow_id),
                    )
            else:
                _mark_processed(cur, flow_id, tags)

        conn.commit()
    return detected


def _mark_processed(cur, flow_id, tags: list):
    cur.execute(
        "UPDATE flows SET tags = %s WHERE id = %s",
        (tags + [PROCESSED_TAG], flow_id),
    )


def main():
    log.info("Запуск flag-decoder (FLAG_REGEX=%s, інтервал=%ds)", FLAG_REGEX, INTERVAL)
    try:
        pattern = re.compile(FLAG_REGEX)
    except re.error as exc:
        log.error("Невалідний FLAG_REGEX %r: %s", FLAG_REGEX, exc)
        raise SystemExit(1)

    conn = None
    has_flags_col = True

    while True:
        try:
            if conn is None or conn.closed:
                log.info("Підключення до TimescaleDB…")
                conn = psycopg2.connect(TIMESCALE)
                has_flags_col = check_schema(conn)
                log.info("Підключено. has_flags_col=%s", has_flags_col)

            detected = process_batch(conn, pattern, has_flags_col)
            if detected:
                log.info("Цикл завершено: знайдено закодованих прапорів у %d потоках.", detected)

        except psycopg2.OperationalError as exc:
            log.warning("Помилка підключення: %s — повтор через 10 с", exc)
            conn = None
            time.sleep(10)
            continue
        except Exception as exc:
            log.exception("Неочікувана помилка: %s", exc)
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    conn = None

        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
