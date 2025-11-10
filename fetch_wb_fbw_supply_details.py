#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Выгрузка деталей поставок FBW (Supplies API) в Supabase/public.
Берём список ключей из public.fbw_supplies и по каждому тянем GET /api/v1/supplies/{ID}
с корректным флагом isPreorderID. Лимит WB: 30 req/min → ставим интервалы.
Ежедневный полный refresh (delete -> insert).
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
SUPPLIES_TABLE        = os.getenv("SUPABASE_SUPPLIES_TABLE", "fbw_supplies")        # источник ID
DETAILS_TABLE         = os.getenv("SUPABASE_DETAILS_TABLE",  "fbw_supply_details")  # приёмник

# Rate limits WB: 30 req/min → ~1 запрос каждые 2 сек с небольшим запасом
REQUEST_SLEEP_SEC     = float(os.getenv("REQUEST_SLEEP_SEC", "2.1"))

API_BASE = "https://supplies-api.wildberries.ru/api/v1"
HEADERS = {
    "Authorization": WB_SUPPLIES_TOKEN or "",
    "Content-Type": "application/json",
}

# ===== Справочник статусов для денормализации (резерв на случай пустого join)
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

# ===== Helpers =====
def fail(msg: str, code: int = 1):
    print(f"ERROR: {msg}", file=sys.stderr)
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

def fetch_details_by_id(id_value: int, is_preorder: bool) -> Dict[str, Any]:
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
            # может быть, старая запись — просто пропустим
            return None
        if resp.status_code == 429 and attempt < len(backoffs):
            continue
        # другие ошибки — фейлим с текстом WB
        fail(f"WB API {resp.status_code}: {resp.text}")
    return None

def normalize_detail(wb_key: str, is_preorder: bool, supply_id: int, preorder_id: int, r: Dict[str, Any]) -> Dict[str, Any]:
    """
    Приводим ответ WB к структуре таблицы.
    """
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

# ===== Main =====
def main():
    if not WB_SUPPLIES_TOKEN:
        fail("WB_SUPPLIES_TOKEN is empty")
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        fail("Supabase URL or SERVICE KEY is empty")

    sb: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

    # 1) Список ключей для детализации из public.fbw_supplies
    #    Берём wb_key, supply_id, preorder_id
    src = sb.schema(SCHEMA).table(SUPPLIES_TABLE).select("wb_key,supply_id,preorder_id").execute()
    rows = getattr(src, "data", None) or src.data  # совместимость разных версий
    keys: List[Dict[str, Any]] = rows if isinstance(rows, list) else []

    print(f"Keys to detail: {len(keys)}")

    details: List[Dict[str, Any]] = []
    processed = 0

    for k in keys:
        wb_key = k.get("wb_key")
        supply_id = k.get("supply_id")
        preorder_id = k.get("preorder_id")

        is_preorder, id_value = parse_wb_key(wb_key)

        # запрос деталей
        data = fetch_details_by_id(id_value, is_preorder)
        if data is None:
            # пропускаем отсутствующие/удалённые
            processed += 1
            if REQUEST_SLEEP_SEC > 0:
                time.sleep(REQUEST_SLEEP_SEC)
            continue

        details.append(normalize_detail(
            wb_key=wb_key,
            is_preorder=is_preorder,
            supply_id=supply_id,
            preorder_id=preorder_id,
            r=data
        ))

        processed += 1
        if processed % 50 == 0:
            print(f"Processed {processed}/{len(keys)}")
        if REQUEST_SLEEP_SEC > 0:
            time.sleep(REQUEST_SLEEP_SEC)

    print(f"Collected details: {len(details)}")

    # 2) Полный refresh таблицы деталей
    sb.schema(SCHEMA).table(DETAILS_TABLE).delete().neq("wb_key", "").execute()

    inserted = 0
    for batch in chunked(details, 500):
        sb.schema(SCHEMA).table(DETAILS_TABLE).insert(batch).execute()
        inserted += len(batch)

    print(f"Inserted rows: {inserted}")
    print("Details sync OK")

if __name__ == "__main__":
    main()
