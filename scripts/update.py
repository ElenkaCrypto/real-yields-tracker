import json
import pathlib
import sys
import time
from datetime import datetime, timezone

import requests

LLAMA_YIELDS_URL = "https://yields.llama.fi/pools"

# Фільтри/налаштування: можна звузити до потрібних чейнів/протоколів
ALLOW_CHAINS = {"Ethereum", "Arbitrum", "Base", "Optimism"}
TOP_N = 50  # скільки пулів брати в снапшоті

def fetch_llama_yields():
    """Реальні дані з DefiLlama Yields API (без ключів)."""
    # трішки ретраїв для надійності
    for i in range(3):
        try:
            r = requests.get(LLAMA_YIELDS_URL, timeout=25)
            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict) and "data" in data:
                return data["data"]
            # старий формат/невідомий: пробуємо як масив
            if isinstance(data, list):
                return data
            raise ValueError("Unexpected API response shape")
        except Exception as e:
            if i == 2:
                raise
            time.sleep(1 + i)
    return []

def normalize(pools_raw):
    """Нормалізуємо тільки суттєве, фільтруємо, сортуємо по TVL."""
    rows = []
    for p in pools_raw:
        try:
            chain = p.get("chain")
            if ALLOW_CHAINS and chain not in ALLOW_CHAINS:
                continue
            rows.append({
                "project": p.get("project"),
                "chain": chain,
                "symbol": p.get("symbol"),
                "apy": p.get("apy"),
                "apyBase": p.get("apyBase"),
                "apyReward": p.get("apyReward"),
                "tvlUsd": p.get("tvlUsd"),
                "pool": p.get("pool"),
                "url": p.get("url") or p.get("urlPool"),
                "ilRisk": p.get("ilRisk"),  # може бути None/“yes”/“no”
            })
        except Exception:
            continue
    # сортуємо за TVL та обрізаємо TOP_N
    rows.sort(key=lambda r: (r["tvlUsd"] or 0), reverse=True)
    return rows[:TOP_N]

def write_snapshot(data_norm):
    dt = datetime.now(timezone.utc)
    folder = pathlib.Path("data") / dt.strftime("%Y-%m-%d")
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{dt.strftime('%H%M%S')}.json"  # унікально до секунд
    payload = {
        "ts": dt.isoformat(timespec="seconds"),
        "source": "DefiLlama Yields",
        "filters": {
            "chains": sorted(list(ALLOW_CHAINS)) if ALLOW_CHAINS else None,
            "top_n": TOP_N,
        },
        "rows": data_norm,
    }
    path.write_text(json.dumps(payload, indent=2))
    return path

if __name__ == "__main__":
    raw = fetch_llama_yields()
    norm = normalize(raw)
    out = write_snapshot(norm)
    print(f"[update.py] wrote file: {out}")
    sys.exit(0)
