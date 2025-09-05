# -*- coding: utf-8 -*-
import os, re, json, traceback
import pandas as pd
import pandasdmx as sdmx
from flask import Flask, request, Response, jsonify

# ==== MSAL（设备码登录 + 缓存 + 静默续期）====
from msal import PublicClientApplication, SerializableTokenCache

# --- IMF Azure B2C 配置（必要） ---
CLIENT_ID = os.getenv("IMF_CLIENT_ID", "446ce2fa-88b1-436c-b8e6-94491ca4f6fb")
AUTHORITY = os.getenv(
    "IMF_AUTHORITY",
    "https://imfprdb2c.b2clogin.com/imfprdb2c.onmicrosoft.com/b2c_1a_signin_aad_simple_user_journey/",
)
SCOPE = os.getenv(
    "IMF_SCOPE",
    "https://imfprdb2c.onmicrosoft.com/4042e178-3e2f-4ff9-ac38-1276c901c13d/iData.Login",
)

# --- 缓存与临时文件（建议挂载持久盘 /var/data） ---
TOKEN_CACHE_PATH = os.getenv("TOKEN_CACHE_PATH", "/var/data/token_cache.json")
DEVICE_FLOW_PATH = os.getenv("DEVICE_FLOW_PATH", "/var/data/device_flow.json")
os.makedirs(os.path.dirname(TOKEN_CACHE_PATH), exist_ok=True)

_token_cache = SerializableTokenCache()
if os.path.exists(TOKEN_CACHE_PATH):
    try:
        _token_cache.deserialize(open(TOKEN_CACHE_PATH, "r", encoding="utf-8").read())
    except Exception:
        pass

app_msal = PublicClientApplication(CLIENT_ID, authority=AUTHORITY, token_cache=_token_cache)

def _persist_cache():
    try:
        open(TOKEN_CACHE_PATH, "w", encoding="utf-8").write(_token_cache.serialize())
    except Exception:
        pass

def _auth_header():
    """静默获取/续期 token；若没有有效 token 则抛错。"""
    accts = app_msal.get_accounts()
    res = app_msal.acquire_token_silent([SCOPE], account=accts[0] if accts else None)
    if not res or "access_token" not in res:
        raise RuntimeError("No valid token in cache. Call /auth/device/start then POST /auth/device/finish once.")
    _persist_cache()
    return {"Authorization": f"{res['token_type']} {res['access_token']}"}

# ==== 业务默认参数 ====
DATASET   = os.getenv("DATASET", "IL")    # IMF 数据流：International Liquidity
DEFAULT_COUNTRY = os.getenv("COUNTRY", "MRT")
DEFAULT_FREQ    = os.getenv("FREQ", "M")  # A/Q/M
DEFAULT_START   = os.getenv("START", "2000")
DECIMALS        = int(os.getenv("DECIMALS", "0"))

# 如需固定指标可写在这里；不传时自动从 codelist 读取
INDICATORS_DEFAULT = [
    "RXDR_REVS","TRGMV_REVS","TRGNV_REVS","RXF11_REVS","RGOLDMV_REVS","RGOLDNV_REVS",
    "TRG35XDR_REVS","TRRPIMF_REVS","RXF11FX_REVS","RXF11ORA_REVS",
    "NFA_ACO_NRES_ODCORP","NFA_LT_NRES_ODCORP",
    "NFA_ACO_NRES_S12R","NFA_LT_NRES_S12R",
    "NFAOFA_ACO_NRES_S121","NFAOFL_LT_NRES_S121",
    "USD_OZG_MR","XDR_OZG_MR"
]

FREQ_NAME = {"A": "Annual", "Q": "Quarterly", "M": "Monthly"}

# ==== 工具函数 ====
def _code_id(v):
    """把 pandasdmx 的 Code/维度对象统一转成字符串 ID。"""
    if hasattr(v, "id"):   return v.id
    if hasattr(v, "code"): return v.code
    s = str(v)
    m = re.search(r"id='?([A-Za-z0-9_]+)'?", s)
    return m.group(1) if m else s

def normalize_timeperiod(series: pd.Series, freq: str) -> pd.Series:
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
            yy = m.loc[mask, 0].astype(int)
            mo = m.loc[mask, 1].astype(int)
            first_mm = ((mo - 1) // 3) * 3 + 1
            out.loc[mask] = yy.astype(str) + "-" + first_mm.map("{:02d}".format)
        m2 = out.str.extract(r"^(\d{4})-(\d{2})$", expand=True)
        mask2 = m2[0].notna()
        if mask2.any():
            yy2 = m2.loc[mask2, 0].astype(int)
            mo2 = m2.loc[mask2, 1].astype(int)
            first_mm2 = ((mo2 - 1) // 3) * 3 + 1
            out.loc[mask2] = yy2.astype(str) + "-" + first_mm2.map("{:02d}".format)
        return out
    if freq == "M":
        out = s.copy()
        m = out.str.extract(r"^(\d{4})[-/]?M?(\d{1,2})$", expand=True)
        mask = m[0].notna()
        if mask.any():
            out.loc[mask] = (
                m.loc[mask, 0].astype(int).astype(str)
                + "-"
                + m.loc[mask, 1].astype(int).map("{:02d}".format)
            )
        out = out.str.replace(r"^(\d{4})-(\d{2})-(\d{2})$", r"\1-\2", regex=True)
        return out
    return s

def sort_key_for_date(date_str: str, freq: str) -> int:
    s = str(date_str)
    if freq == "A":
        ts = pd.to_datetime(f"{s}-01-01", errors="coerce")
    else:
        ts = pd.to_datetime(f"{s}-01", errors="coerce")
    return ts.toordinal() if pd.notnull(ts) else -10**12

# ==== 取数主函数（pandasdmx.Request("IMF")）====
def fetch_il_wide(dataset=DATASET, country=DEFAULT_COUNTRY, freq=DEFAULT_FREQ, start=DEFAULT_START, indicators=None) -> pd.DataFrame:
    # 1) SDMX 客户端（pandasdmx 的接口）
    imf = sdmx.Request("IMF")

    # 2) DSD & codelist
    flow = imf.dataflow(dataset)
    dsd_id = flow.dataflow[dataset].structure.id
    sm = imf.datastructure(dsd_id, params={"references": "descendants"})
    dsd_obj = sm.get(dsd_id)
    clmap = getattr(sm, "codelist", {})

    # 3) 指标集合
    inds = indicators if indicators is not None else INDICATORS_DEFAULT
    if inds is None:
        cl_ind = clmap.get("CL_IL_INDICATOR") or clmap.get("IMF.STA:CL_IL_INDICATOR")
        if not cl_ind:
            ind_dim = next((d for d in dsd_obj.dimensions.components if d.id.upper()=="INDICATOR"), None)
            if ind_dim and getattr(ind_dim.local_representation, "enumeration", None):
                cl_id = ind_dim.local_representation.enumeration.id
                cl_ind = sm.codelist.get(cl_id)
        if cl_ind:
            inds = [ (getattr(c,"id",None) or getattr(c,"code",None)) for c in getattr(cl_ind,"codes",[]) ]
            inds = [str(x) for x in inds if x]
        else:
            raise RuntimeError("Cannot load CL_IL_INDICATOR; please set INDICATORS manually.")

    # 4) 组 key 并取数（带 IMF 授权头）
    key = {"FREQUENCY": freq, "COUNTRY": country, "INDICATOR": inds}

    dm = imf.data(
        dataset,
        key=key,
        params={"startPeriod": start, "detail": "dataonly"},
        headers=_auth_header(),
        dsd=dsd_obj,
    )

    # 5) 转 pandas 宽表
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
    wide["__sort"] = wide["Date"].map(lambda x: sort_key_for_date(x, freq))
    wide = wide.sort_values("__sort").drop(columns="__sort").reset_index(drop=True)

    keep = ["Date"] + [c for c in wide.columns if c != "Date" and pd.to_numeric(wide[c], errors="coerce").notna().any()]
    return wide[keep]

# ==== Flask App ====
app = Flask(__name__)

# 根路由：简单提示
@app.get("/")
def index():
    return jsonify({
        "service": "IMF IL API",
        "endpoints": {
            "health": "/health",
            "auth_start": "/auth/device/start",
            "auth_finish": "POST /auth/device/finish",
            "debug_token": "/debug/token_ok",
            "data": "/api/il_wide?country=MRT&freq=M&start=2000&format=csv|json"
        }
    })

# 健康检查：显示 token 是否可用
@app.get("/health")
def health():
    ready = False
    try:
        _ = _auth_header()
        ready = True
    except Exception:
        ready = False
    return jsonify({"dataset": DATASET, "status": "ok", "token_ready": ready})

# 设备码登录：开始
@app.get("/auth/device/start")
def auth_device_start():
    try:
        flow = app_msal.initiate_device_flow(scopes=[SCOPE])
        if not flow or "user_code" not in flow:
            return jsonify({"error": "device_flow_failed", "details": flow}), 500
        os.makedirs(os.path.dirname(DEVICE_FLOW_PATH), exist_ok=True)
        with open(DEVICE_FLOW_PATH, "w", encoding="utf-8") as f:
            json.dump(flow, f)
        return jsonify({
            "verification_uri": flow.get("verification_uri"),
            "user_code": flow.get("user_code"),
            "message": flow.get("message", "Open verification_uri and enter user_code, then call POST /auth/device/finish")
        })
    except Exception as e:
        return jsonify({"error": "exception", "message": str(e), "trace": traceback.format_exc().splitlines()[-6:]}), 500

# 设备码登录：结束（需 POST）
@app.post("/auth/device/finish")
def auth_device_finish():
    try:
        if not os.path.exists(DEVICE_FLOW_PATH):
            return jsonify({"error": "no_pending_flow", "hint": "Call /auth/device/start first."}), 400
        flow = json.load(open(DEVICE_FLOW_PATH, "r", encoding="utf-8"))
        result = app_msal.acquire_token_by_device_flow(flow)  # 阻塞直到完成/超时
        if "access_token" not in result:
            return jsonify({"error": "auth_failed", "details": result}), 500
        _persist_cache()
        try:
            os.remove(DEVICE_FLOW_PATH)
        except Exception:
            pass
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": "exception", "message": str(e), "trace": traceback.format_exc().splitlines()[-6:]}), 500

# 调试：检查 token 是否有效
@app.get("/debug/token_ok")
def debug_token_ok():
    try:
        _ = _auth_header()
        return jsonify({"ok": True, "cache_path": TOKEN_CACHE_PATH})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "cache_path": TOKEN_CACHE_PATH}), 500

# 数据接口：CSV/JSON
@app.get("/api/il_wide")
def api_il_wide():
    """
    参数：
      - country: 默认 MRT
      - freq: A/Q/M，默认 M
      - start: 默认 2000
      - indicators: 逗号分隔（可选；默认 INDICATORS_DEFAULT）
      - format: csv|json，默认 csv
    """
    try:
        country = (request.args.get("country") or DEFAULT_COUNTRY).upper().strip()
        freq    = (request.args.get("freq") or DEFAULT_FREQ).upper().strip()
        start   = (request.args.get("start") or DEFAULT_START).strip()
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
    # 本地调试：Render 上用 gunicorn 启动
    app.run(host="0.0.0.0", port=8000, debug=False)
