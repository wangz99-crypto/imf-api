# -*- coding: utf-8 -*-
"""
Created on Thu Sep  4 19:02:07 2025

@author: 23517
"""

# -*- coding: utf-8 -*-
import os, re, json, traceback
import pandas as pd
import pandasdmx as sdmx
from flask import Flask, request, Response, jsonify

# ========= MSAL：设备码登录 + 缓存 + 静默续期 =========
from msal import PublicClientApplication, SerializableTokenCache

CLIENT_ID = os.getenv("IMF_CLIENT_ID", "446ce2fa-88b1-436c-b8e6-94491ca4f6fb")
AUTHORITY = os.getenv("IMF_AUTHORITY", "https://imfprdb2c.b2clogin.com/imfprdb2c.onmicrosoft.com/b2c_1a_signin_aad_simple_user_journey/")
SCOPE = os.getenv("IMF_SCOPE", "https://imfprdb2c.onmicrosoft.com/4042e178-3e2f-4ff9-ac38-1276c901c13d/iData.Login")

# 可持久化的 token 缓存文件（Render 上建议打开 Persistent Disk）
TOKEN_CACHE_PATH = os.getenv("TOKEN_CACHE_PATH", os.path.abspath("./token_cache.json"))
_token_cache = SerializableTokenCache()
if os.path.exists(TOKEN_CACHE_PATH):
    try:
        _token_cache.deserialize(open(TOKEN_CACHE_PATH, "r", encoding="utf-8").read())
    except Exception:
        pass

app_msal = PublicClientApplication(CLIENT_ID, authority=AUTHORITY, token_cache=_token_cache)
_device_flow = None  # 全局保存一次设备码流程（/auth/device/start 触发）

def _save_cache():
    try:
        open(TOKEN_CACHE_PATH, "w", encoding="utf-8").write(_token_cache.serialize())
    except Exception:
        pass

def _auth_header():
    """每次 IMF 请求前静默续期；如果还没登录，会抛错。"""
    accounts = app_msal.get_accounts()
    res = app_msal.acquire_token_silent([SCOPE], account=accounts[0] if accounts else None)
    if not res or "access_token" not in res:
        raise RuntimeError("No valid token. Call /auth/device/start then /auth/device/finish to login once.")
    _save_cache()
    return {'Authorization': f"{res['token_type']} {res['access_token']}"}

# ========== 默认业务参数 ==========
DATASET = os.getenv("DEFAULT_DATASET", "IL")
COUNTRY = os.getenv("DEFAULT_COUNTRY", "MRT")
FREQ    = os.getenv("DEFAULT_FREQ", "M")
START   = os.getenv("DEFAULT_START", "2000")
DECIMALS = int(os.getenv("DECIMALS", "0"))

INDICATORS = [
    "RXDR_REVS","TRGMV_REVS","TRGNV_REVS","RXF11_REVS","RGOLDMV_REVS","RGOLDNV_REVS",
    "TRG35XDR_REVS","TRRPIMF_REVS","RXF11FX_REVS","RXF11ORA_REVS",
    "NFA_ACO_NRES_ODCORP","NFA_LT_NRES_ODCORP",
    "NFA_ACO_NRES_S12R","NFA_LT_NRES_S12R",
    "NFAOFA_ACO_NRES_S121","NFAOFL_LT_NRES_S121",
    "USD_OZG_MR","XDR_OZG_MR"
]
FREQ_NAME = {"A": "Annual", "Q": "Quarterly", "M": "Monthly"}

# ========== 工具函数 ==========
def _code_id(v):
    if hasattr(v, "id"):   return v.id
    if hasattr(v, "code"): return v.code
    s = str(v)
    m = re.search(r"id='?([A-Za-z0-9_]+)'?", s)
    return m.group(1) if m else s

def normalize_timeperiod(series, freq):
    s = series.astype(str)
    if freq == "A":
        return s.str.extract(r"(\d{4})", expand=False).fillna(s)
    if freq == "Q":
        out = s.copy()
        for q, mm in {"1":"01","2":"04","3":"07","4":"10"}.items():
            out = out.str.replace(rf"^(\d{{4}})-?Q{q}$", rf"\1-{mm}", regex=True)
        m = out.str.extract(r"^(\d{4})[-/]?M?(\d{1,2})$", expand=True)
        mask = m[0].notna()
        if mask.any():
            yy = m.loc[mask, 0].astype(int); mo = m.loc[mask, 1].astype(int)
            first_mm = ((mo - 1) // 3) * 3 + 1
            out.loc[mask] = yy.astype(str) + "-" + f"{first_mm:02d}"
        m2 = out.str.extract(r"^(\d{4})-(\d{2})$", expand=True)
        mask2 = m2[0].notna()
        if mask2.any():
            yy2 = m2.loc[mask2, 0].astype(int); mo2 = m2.loc[mask2, 1].astype(int)
            first_mm2 = ((mo2 - 1) // 3) * 3 + 1
            out.loc[mask2] = yy2.astype(str) + "-" + f"{first_mm2:02d}"
        return out
    if freq == "M":
        out = s.copy()
        m = out.str.extract(r"^(\d{4})[-/]?M?(\d{1,2})$", expand=True)
        mask = m[0].notna()
        if mask.any():
            out.loc[mask] = m.loc[mask, 0].astype(int).astype(str) + "-" + m.loc[mask, 1].astype(int).map("{:02d}".format)
        out = out.str.replace(r"^(\d{4})-(\d{2})-(\d{2})$", r"\1-\2", regex=True)
        return out
    return s

def sort_key_for_date(date_str, freq):
    ts = pd.to_datetime(f"{date_str}-01-01" if freq == "A" else f"{date_str}-01", errors="coerce")
    return ts.toordinal() if pd.notnull(ts) else -10**12

# ========== 主函数 ==========
def fetch_il_wide(dataset=DATASET, country=COUNTRY, freq=FREQ, start=START, indicators=None):
    imf = sdmx.Client("IMF_DATA")
    flow = imf.dataflow(dataset)
    dsd_id = flow.dataflow[dataset].structure.id
    sm = imf.datastructure(dsd_id, params={"references": "descendants"})
    dsd_obj = sm.get(dsd_id)

    inds = indicators if indicators is not None else INDICATORS
    key = {"FREQUENCY": freq, "COUNTRY": country, "INDICATOR": inds}

    dm = imf.data(
        dataset,
        key=key,
        params={"startPeriod": start, "detail": "dataonly"},
        headers=_auth_header(),   # 静默续期
        dsd=dsd_obj,
    )

    obj = sdmx.to_pandas(dm)
    if obj is None or (hasattr(obj, "size") and obj.size == 0):
        return pd.DataFrame(columns=["Date"])

    df = obj.rename("value").reset_index() if isinstance(obj, pd.Series) else obj.reset_index()
    need = [c for c in ["TIME_PERIOD","FREQUENCY","COUNTRY","INDICATOR","value"] if c in df.columns]
    df = df[need].copy()

    for c in ("COUNTRY","INDICATOR","FREQUENCY"):
        if c in df.columns:
            df[c] = df[c].map(_code_id)

    if "FREQUENCY" in df.columns:
        df = df[df["FREQUENCY"].astype(str).str.upper().eq(freq)]
    if "COUNTRY" in df.columns:
        df = df[df["COUNTRY"].astype(str).str.upper().eq(country)]
    if df.empty:
        return pd.DataFrame(columns=["Date"])

    df["Date"] = normalize_timeperiod(df["TIME_PERIOD"], freq)

    wide = df.pivot_table(index="Date", columns="INDICATOR", values="value", aggfunc="first").reset_index()
    cols = ["Date"] + sorted([c for c in wide.columns if c != "Date"])
    wide = wide[cols]
    wide["__sort"] = wide["Date"].map(lambda x: sort_key_for_date(x, freq))
    wide = wide.sort_values("__sort").drop(columns="__sort").reset_index(drop=True)

    keep = ["Date"] + [c for c in wide.columns if c != "Date" and pd.to_numeric(wide[c], errors="coerce").notna().any()]
    return wide[keep]

# ========== Flask ==========
app = Flask(__name__)

@app.get("/health")
def health():
    ok = True
    try:
        _auth_header()
    except Exception:
        ok = False
    return jsonify({"status": "ok", "dataset": DATASET, "token_ready": ok})

# —— 设备码登录：启动 & 完成 —— #
@app.get("/auth/device/start")
def auth_device_start():
    """返回 verification_uri 和 user_code；在浏览器完成授权后再调用 /auth/device/finish。"""
    global _device_flow
    _device_flow = app_msal.initiate_device_flow(scopes=[SCOPE])
    if "user_code" not in _device_flow:
        return jsonify({"error": "device_flow_init_failed", "detail": _device_flow}), 500
    # 不要在这里阻塞等待；先把信息返回给你
    return jsonify({
        "message": "Open verification_uri in browser and enter user_code, then call POST /auth/device/finish",
        "verification_uri": _device_flow.get("verification_uri") or _device_flow.get("verification_url"),
        "user_code": _device_flow["user_code"]
    })

@app.post("/auth/device/finish")
def auth_device_finish():
    """在你完成验证码输入后调用一次；会阻塞几秒～几十秒直到成功或超时。"""
    global _device_flow
    if not _device_flow:
        return jsonify({"error": "no_active_flow", "hint": "Call /auth/device/start first"}), 400
    res = app_msal.acquire_token_by_device_flow(_device_flow)  # 内部轮询
    _device_flow = None
    if "access_token" not in res:
        return jsonify({"ok": False, "error": str(res)}), 500
    _save_cache()
    return jsonify({"ok": True, "cached_to": TOKEN_CACHE_PATH})

@app.get("/debug/token_ok")
def debug_token_ok():
    try:
        _auth_header()
        return jsonify({"ok": True, "cache_path": TOKEN_CACHE_PATH})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "cache_path": TOKEN_CACHE_PATH}), 401

@app.get("/api/il_wide")
def api_il_wide():
    """
    参数：
      - country: 默认 MRT
      - freq: A/Q/M，默认 M
      - start: 默认 2000
      - indicators: 逗号分隔（可选；默认用脚本里的 INDICATORS 列表）
      - format: csv|json，默认 csv
    """
    try:
        country = (request.args.get("country") or COUNTRY).upper().strip()
        freq    = (request.args.get("freq") or FREQ).upper().strip()
        start   = (request.args.get("start") or START).strip()
        fmt     = (request.args.get("format") or "csv").lower().strip()
        inds    = request.args.get("indicators", "").strip()
        indicators = [x.strip() for x in inds.split(",") if x.strip()] if inds else None

        df = fetch_il_wide(dataset=DATASET, country=country, freq=freq, start=start, indicators=indicators)

        if df.empty:
            msg = {"error":"no_data","hint":{"dataset":DATASET,"country":country,"freq":freq,"start":start,"indicators":indicators}}
            if fmt == "json":
                return jsonify(msg), 424
            return Response("", status=204)

        if fmt == "json":
            return jsonify(df.to_dict(orient="records"))

        csv_text = df.to_csv(index=False, encoding="utf-8-sig", float_format=f"%.{DECIMALS}f")
        return Response(
            csv_text,
            mimetype="text/csv",
            headers={
                "Content-Disposition": f'inline; filename="{country}_{DATASET}_{freq}_{start}_wide.csv"',
                "Cache-Control": "no-store",
            },
        )
    except Exception as e:
        return jsonify({"error": str(e), "trace": traceback.format_exc().splitlines()[-8:]}), 500

if __name__ == "__main__":
    # 本地调试
    app.run(host="0.0.0.0", port=8000, debug=False)

