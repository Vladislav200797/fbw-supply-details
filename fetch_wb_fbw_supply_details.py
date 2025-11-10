#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Выгрузка деталей поставок FBW (Supplies API) в Supabase/public.
Берём список ключей из public.fbw_supplies (можно отфильтровать по updated_date за N дней),
по каждому тянем GET /api/v1/supplies/{ID}?isPreorderID=...
С учётом лимита 30 req/min ставим паузу REQUEST_SLEEP_SEC.
Ежедневный полный refresh (delete -> insert).

Добавлено:
- неблокирующий вывод (используйте python -u и/или PYTHONUNBUFFERED=1);
- частый прогресс-лог (LOG_EVERY, дефолт 25);
- DETAILS_UPDATED_DAYS: фильтр ключей по updated_date;
- MAX_KEYS: ограничение количества ключей за запуск (для тестов/ускорения).
"""

import os
import sys
import time
from typing import List, Dict, Any, Tuple

import requests
from supabase import create_client, Client

# ===== ENV =====
WB_SUPPLIES_TOKEN     = os.getenv("WB_SUPPLIES_TOKEN")            # HeaderApiKey
SUPABASE_URL          = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY  = os.getenv("SUPABASE_SERVICE_KEY")          # service_role
SCHEMA                = os.getenv("SUPABASE_SCHEMA", "public")
SUPPLIES_TABLE        = os.getenv("SUPABASE_SUPPLIES_TABLE", "fbw_supplies")
DETAILS_TABLE         = os.getenv("SUPABASE_DETAILS_TABLE",  "fbw_supply_details")

# Rate limits WB: 30 req/min → ~1 запрос каждые 2 сек (чуть с запасом)
REQUEST_SLEEP_SEC     = float(os.getenv("REQUEST_SLEEP_SEC", "2.1"))

# Фильтрация исходных ключей:
DETAILS_UPDATED_DAYS  = os.getenv("DETAILS_UPDATED_DAYS", "").strip()  # например "7"
MAX_KEYS_ENV          = os.getenv("MAX_KEYS", "").strip()              # например "500"
LOG_EVERY_ENV         = os.getenv("LOG_EVERY", "").strip()             # например "25"

API_BASE = "https://supplies-api.wildberries.ru/api/v1"
HEADERS = {
    "Authorization": WB_SUPPLIES_TOKEN or "",
    "Content-Type": "application/json",
}

# ===== Справочник статусов (для денормализации в таблицу) =====
STATUS_NAME_RU = {
    1: "Не запланировано",
    2: "Запланировано",
    3: "Отгрузка разрешена",
    4: "Идёт приёмка",
    5: "Принято",
    6: "Отгружено на воротах",
}
STATUS_NAME_EN = {
    1: "Not planned",
    2: "Planned",
    3: "Shipping allowed",
    4: "Acceptance in progress",
    5: "Accepted",
    6: "Shipped at gates",
}

def fail(msg: str, code: int = 1):
    print(f"ERROR: {msg}", file=sys.stderr, flush=True)
    sys.exit(code)

def parse_wb_key(wb_key: str) -> Tuple[bool, int]:
    """
    wb_key формата 'S:<id>' или 'P:<id>'.
    Возвращает (is_preorder, id_int).
    """
    if wb_key.startswith("S:"):
        return (False, int(wb_key[2:]))
    if wb_key.startswith("P:"):
        return (True, int(wb_key[2:]))
    raise ValueError(f"Bad wb_key format: {wb_key}")

def fetch_details_by_id(id_value: int, is_preorder: bool) -> Dict[str, Any] | None:
    """
    GET /api/v1/supplies/{ID}?isPreorderID=<true|false>
    Ретраим 429/временные ошибки.
    """
    url = f"{API_BASE}/supplies/{id_value}"
    params = {"isPreorderID": "true" if is_preorder else "false"}

    backoffs = [0, 2, 5]
    for attempt, wait in enumerate(backoffs, start=1):
        if wait:
            time.sleep(wait)
        resp = requests.get(url, headers=HEADERS, params=params, timeout=60)
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code in (404, 410):
            # старая/удалённая — пропускаем
            return None
        if resp.status_code == 429 and attempt < len(backoffs):
            continue
        fail(f"WB API {resp.status_code}: {resp.text}")
    return None

def normalize_detail(wb_key: str, is_preorder: bool, supply_id: int, preorder_id: int, r: Dict[str, Any]) -> Dict[str, Any]:
    status_id = r.get("statusID")
    return {
        "wb_key": wb_key,
        "is_preorder": is_preorder,
        "supply_id": supply_id,
        "preorder_id": preorder_id,

        "phone": r.get("phone"),
        "status_id": status_id,
        "status_name_ru": STATUS_NAME_RU.get(status_id),
        "status_name_en": STATUS_NAME_EN.get(status_id),

        "virtual_type_id": r.get("virtualTypeID"),
        "box_type_id": r.get("boxTypeID"),

        "create_date": r.get("createDate"),
        "supply_date": r.get("supplyDate"),
        "fact_date": r.get("factDate"),
        "updated_date": r.get("updatedDate"),

        "warehouse_id": r.get("warehouseID"),
        "warehouse_name": r.get("warehouseName"),
        "actual_warehouse_id": r.get("actualWarehouseID"),
        "actual_warehouse_name": r.get("actualWarehouseName"),
        "transit_warehouse_id": r.get("transitWarehouseID"),
        "transit_warehouse_name": r.get("transitWarehouseName"),

        "acceptance_cost": r.get("acceptanceCost"),
        "paid_acceptance_coefficient": r.get("paidAcceptanceCoefficient"),
        "reject_reason": r.get("rejectReason"),
        "supplier_assign_name": r.get("supplierAssignName"),
        "storage_coef": r.get("storageCoef"),
        "delivery_coef": r.get("deliveryCoef"),

        "quantity": r.get("quantity"),
        "ready_for_sale_quantity": r.get("readyForSaleQuantity"),
        "accepted_quantity": r.get("acceptedQuantity"),
        "unloading_quantity": r.get("unloadingQuantity"),
        "depersonalized_quantity": r.get("depersonalizedQuantity"),
    }

def chunked(seq: List[Dict[str, Any]], size: int):
    for i in range(0, len(seq), size):
        yield seq[i:i+size]

def main():
    if not WB_SUPPLIES_TOKEN:
        fail("WB_SUPPLIES_TOKEN is empty")
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        fail("Supabase URL or SERVICE KEY is empty")

    sb: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

    # ==== 1) Подготовим выборку ключей из fbw_supplies ====
    query = sb.schema(SCHEMA).table(SUPPLIES_TABLE).select("wb_key,supply_id,preorder_id,updated_date")

    # Фильтр по обновлению за последние N дней (ускоряет работу)
    if DETAILS_UPDATED_DAYS:
        try:
            ndays = int(DETAILS_UPDATED_DAYS)
            # В PostgREST можно использовать gte на текстовом timestamptz, но надёжнее фильтровать на стороне PG.
            # Упростим: пусть всё равно отдаст нам всё, а мы отфильтруем в Python — но сначала попробуем серверный фильтр.
            # Если у тебя включён PostgREST 11+, строка ниже сработает:
            from datetime import datetime, timedelta, timezone
            msk_now = datetime.now(timezone.utc)  # PG хранит в UTC; фильтрация по UTC тоже ок
            cutoff = (msk_now - timedelta(days=ndays)).isoformat(timespec="seconds").replace("+00:00", "Z")
            query = query.gte("updated_date", cutoff)
        except Exception:
            pass

    resp = query.execute()
    src_rows = getattr(resp, "data", None) or resp.data
    keys: List[Dict[str, Any]] = src_rows if isinstance(src_rows, list) else []

    # Клиентский доп.фильтр (на всякий случай)
    if DETAILS_UPDATED_DAYS:
        try:
            ndays = int(DETAILS_UPDATED_DAYS)
            from datetime import datetime, timezone, timedelta
            cutoff_dt = datetime.now(timezone.utc) - timedelta(days=ndays)
            # updated_date в таблице timestamptz → Supabase вернёт ISO-строку
            filtered = []
            for k in keys:
                ud = k.get("updated_date")
                if not ud:
                    continue
                try:
                    # Разрешим разные форматы Z/+00:00
                    iso = ud.replace("Z", "+00:00") if isinstance(ud, str) else ud
                    dt = datetime.fromisoformat(iso)
                    if dt >= cutoff_dt:
                        filtered.append(k)
                except Exception:
                    filtered.append(k)
            keys = filtered
        except Exception:
            pass

    # Лимит для прогона
    max_keys = None
    if MAX_KEYS_ENV:
        try:
            max_keys = int(MAX_KEYS_ENV)
        except Exception:
            max_keys = None
    if max_keys is not None:
        keys = keys[:max_keys]

    total = len(keys)
    log_every = 25
    if LOG_EVERY_ENV:
        try:
            log_every = max(1, int(LOG_EVERY_ENV))
        except Exception:
            pass

    print(f"Keys to detail: {total} (updated<= {DETAILS_UPDATED_DAYS or 'ALL'} days, limit={max_keys or '∞'})", flush=True)

    # Нет ключей — моментальный выход с очисткой/без (лучше просто не трогать таблицу)
    if total == 0:
        print("No keys to process. Skipping refresh.", flush=True)
        return

    details: List[Dict[str, Any]] = []
    processed = 0
    errors = 0

    # ==== 2) Обход ID ====
    for k in keys:
        wb_key = k.get("wb_key")
        supply_id = k.get("supply_id")
        preorder_id = k.get("preorder_id")

        try:
            is_preorder, id_value = parse_wb_key(wb_key)
            data = fetch_details_by_id(id_value, is_preorder)
            if data is not None:
                details.append(normalize_detail(
                    wb_key=wb_key,
                    is_preorder=is_preorder,
                    supply_id=supply_id,
                    preorder_id=preorder_id,
                    r=data
                ))
        except Exception as e:
            errors += 1
            print(f"[WARN] {wb_key}: {e}", flush=True)

        processed += 1
        if processed % log_every == 0 or processed == total:
            print(f"Processed {processed}/{total} (collected {len(details)}, errors {errors})", flush=True)

        if REQUEST_SLEEP_SEC > 0:
            time.sleep(REQUEST_SLEEP_SEC)

    print(f"Collected details: {len(details)}; errors: {errors}", flush=True)

    # ==== 3) Полный refresh таблицы ====
    print("Clearing target table...", flush=True)
    sb.schema(SCHEMA).table(DETAILS_TABLE).delete().neq("wb_key", "").execute()

    print("Inserting data...", flush=True)
    inserted = 0
    for batch in chunked(details, 500):
        sb.schema(SCHEMA).table(DETAILS_TABLE).insert(batch).execute()
        inserted += len(batch)

    print(f"Inserted rows: {inserted}", flush=True)
    print("Details sync OK", flush=True)

if __name__ == "__main__":
    main()
