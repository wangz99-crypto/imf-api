# -*- coding: utf-8 -*-
import os, re, json, base64, traceback
import pandas as pd
import pandasdmx as sdmx
from flask import Flask, request, Response, jsonify
from msal import PublicClientApplication, SerializableTokenCache

# ===== IMF B2C 基本配置 =====
CLIENT_ID = os.getenv("IMF_CLIENT_ID", "446ce2fa-88b1-436c-b8e6-94491ca4f6fb")
AUTHORITY = os.getenv(
    "IMF_AUTHORITY",
    "https://imfprdb2c.b2clogin.com/imfprdb2c.onmicrosoft.com/b2c_1a_signin_aad_simple_user_journey/",
)
SCOPE = os.getenv(
    "IMF_SCOPE",
    "https://imfprdb2c.onmicrosoft.com/4042e178-3e2f-4ff9-ac38-1276c901c13d/iData.Login",
)

# ===== 路径兜底：先 /var/data，失败回退到源码目录，再不行 /tmp =====
def _ensure_writable(path: str, fallback_dir="/opt/render/project/src", last_resort="/tmp") -> str:
    d = os.path.dirname(path) or "."
    try:
        os.makedirs(d, exist_ok=True)
        testfile = os.path.join(d, ".writetest")
        with open(testfile, "w", encoding="utf-8") as f:
            f.write("ok")
        os.remove(testfile)
        return path
    except Exception:
        try:
            os.makedirs(fallback_dir, exist_ok=True)
            return os.path.join(fallback_dir, os.path.basename(path))
        except Exception:
            os.makedirs(last_resort, exist_ok=True)
            return os.path.join(last_resort, os.path.basename(path))

TOKEN_CACHE_PATH = _ensure_writable(os.getenv("TOKEN_CACHE_PATH", "/var/data/token_cache.json"))

# ===== 若提供了 TOKEN_CACHE_B64，就在启动时写入文件 =====
b64 = os.getenv("TOKEN_CACHE_B64")
if b64 and not os.path.exists(TOKEN_CACHE_PATH):
    try:
        with open(TOKEN_CACHE_PATH, "wb") as f:
            f.write(base64.b64decode(b64))
    except Exception:
        pass

# ===== 初始化 MSAL（只用缓存，不交互登录）=====
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
    """从缓存静默取 token；失败则给出 401 提示你更新缓存。"""
    accts = app_msal.get_accounts()
    res = app_msal.acquire_token_silent([SCOPE], account=accts[0] if accts else None)
    if not res or "access_token" not in res:
        raise PermissionError(
            "No valid token in cache. Please run init_token.py locally and upload token_cache.json "
            "(or set TOKEN_CACHE_B64)."
        )
    _persist_cache()
    return {"Authorization": f"{res['token_type']} {res['access_token']}"}

# ===== 业务默认参数 =====
DATASET         = os.getenv("DATASET", "IL")   # IMF International Liquidity
DEFAULT_COUNTRY = os.getenv("COUNTRY", "MRT")
DEFAULT_FREQ    = os.getenv("FREQ", "M")       # A/Q/M
DEFAULT_START   = os.getenv("START", "2000")
DECIMALS        = int(os.getenv("DECIMALS", "0"))

INDICATORS_DEFAULT = [
    "RXDR_REVS","TRGMV_REVS","TRGNV_REVS","RXF11_REVS","RGOLDMV_REVS","RGOLDNV_REVS",
    "TRG35XDR_REVS","TRRPIMF_REVS","RXF11FX_REVS","RXF11ORA_REVS",
    "NFA_ACO_NRES_ODCORP","NFA_LT_NRES_ODCORP",
    "NFA_ACO_NRES_S12R","NFA_LT_NRES_S12R",
    "NFAOFA_ACO_NRES_S121","NFAOFL_LT_NRES_S121",
    "USD_OZG_MR","XDR_OZG_MR"
]

# ===== 工具函数 =====
def _code_id(v):
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
    ts = pd.to_datetime(f"{s}-01-01" if freq == "A" else f"{s}-01", errors="coerce")
    return ts.toordinal() if pd.notnull(ts) else -10**12

# ===== 取数（pandasdmx + IMF）=====
def fetch_il_wide(dataset=DATASET, country=DEFAULT_COUNTRY, freq=DEFAULT_FREQ, start=DEFAULT_START, indicators=None) -> pd.DataFrame:
    imf = sdmx.Request("IMF")
    H = _auth_header()

    # ---------- 读取 dataflow，带授权；404 时回退到“全量+本地挑选” ----------
    try:
        # 首选：只取指定 dataflow（部分 IMF 端点可能报 404）
        flow = imf.dataflow(dataset, headers=H)
    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            # 回退：取全部 dataflow，再挑 dataset
            flow = imf.dataflow(headers=H)
            if dataset not in flow.dataflow:
                # 有些端点 dataflow 的 key 可能是 'IMF:IL' 这类；做一次宽松匹配
                candidates = [k for k in flow.dataflow.keys() if k.split(":")[-1] == dataset]
                if candidates:
                    dataset_key = candidates[0]
                else:
                    raise RuntimeError(f"Dataflow '{dataset}' not found on IMF SDMX.")
            else:
                dataset_key = dataset
        else:
            raise
    else:
        dataset_key = dataset

    dsd_id = flow.dataflow[dataset_key].structure.id

    # ---------- 读取 datastructure，带授权 ----------
    sm = imf.datastructure(dsd_id, params={"references": "descendants"}, headers=H)
    dsd_obj = sm.get(dsd_id)
    clmap = getattr(sm, "codelist", {})

    # ---------- 指标集合 ----------
    inds = indicators if indicators is not None else INDICATORS_DEFAULT
    if inds is None:
        cl_ind = clmap.get("CL_IL_INDICATOR") or clmap.get("IMF.STA:CL_IL_INDICATOR")
        if not cl_ind:
            ind_dim = next((d for d in dsd_obj.dimensions.components if d.id.upper()=="INDICATOR"), None)
            if ind_dim and getattr(ind_dim.local_representation, "enumeration", None):
                cl_id = ind_dim.local_representation.enumeration.id
                cl_ind = sm.codelist.get(cl_id)
        if cl_ind:
            inds = [ (getattr(c,"id",None) or getattr(c,"code",None)) for c in getattr(cl_ind, "codes", []) ]
            inds = [str(x) for x in inds if x]
        else:
            raise RuntimeError("Cannot load CL_IL_INDICATOR; please set INDICATORS manually.")

    # ---------- 拉数（也带授权） ----------
    key = {"FREQUENCY": freq, "COUNTRY": country, "INDICATOR": inds}
    dm = imf.data(
        dataset,  # 注意这里 dataset 仍传 'IL'（IMF 源里会处理）
        key=key,
        params={"startPeriod": start, "detail": "dataonly"},
        headers=H,
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
    wide["__sort"] = wide["Date"].map(lambda x: sort_key_for_date(x, freq))
    wide = wide.sort_values("__sort").drop(columns="__sort").reset_index(drop=True)

    keep = ["Date"] + [c for c in wide.columns if c != "Date" and pd.to_numeric(wide[c], errors="coerce").notna().any()]
    return wide[keep]

# ===== Flask =====
app = Flask(__name__)

@app.get("/")
def index():
    ready = False
    try:
        _ = _auth_header()
        ready = True
    except Exception:
        ready = False
    return jsonify({
        "service": "IMF IL API (cache-only)",
        "health": "/health",
        "data_example": "/api/il_wide?country=MRT&freq=M&start=2000&format=csv",
        "token_ready": ready,
        "token_cache_path": TOKEN_CACHE_PATH
    })

@app.get("/health")
def health():
    try:
        _ = _auth_header()
        return jsonify({"status":"ok","dataset":DATASET,"token_ready":True})
    except Exception as e:
        return jsonify({"status":"ok","dataset":DATASET,"token_ready":False,"hint":str(e)})

@app.get("/debug/token_ok")
def debug_token_ok():
    try:
        _ = _auth_header()
        return jsonify({"ok": True, "cache_path": TOKEN_CACHE_PATH})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "cache_path": TOKEN_CACHE_PATH}), 401

@app.get("/api/il_wide")
def api_il_wide():
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
    except PermissionError as e:
        return jsonify({"error":"no_token","message":str(e)}), 401
    except Exception as e:
        return jsonify({"error": str(e), "trace": traceback.format_exc().splitlines()[-8:]}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=False)


