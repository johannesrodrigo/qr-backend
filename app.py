import os, io, re, time, base64, hmac, hashlib, unicodedata
from typing import Optional
import requests, pandas as pd
from fastapi import FastAPI, Query, Response
from fastapi.responses import HTMLResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware

# ======== Config por variables de entorno =========
SECRET_KEY = os.environ.get("SECRET_KEY", "").encode("utf-8")   # OBLIGATORIO
ONEDRIVE_URL = os.environ.get("ONEDRIVE_URL", "")               # OBLIGATORIO
SHEET_NAME   = os.environ.get("SHEET_NAME", "")                 # opcional (vacío = 1ra hoja)
HEADER_ROW   = int(os.environ.get("HEADER_ROW", "12"))          # 1-based (tú usas 12)
CACHE_TTL    = int(os.environ.get("CACHE_TTL", "60"))           # segundos
ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "*")          # para permitir tu GitHub Pages

if not SECRET_KEY or not ONEDRIVE_URL:
    raise RuntimeError("Faltan SECRET_KEY u ONEDRIVE_URL en variables de entorno.")

# ======== Utilidades ========
def b64url(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).decode().rstrip("=")

def normalize_doc(s: str) -> str:
    s = (s or "").strip().upper()
    return re.sub(r"[^A-Z0-9]", "", s)

def strip_accents(s: str) -> str:
    if not isinstance(s, str): return str(s)
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")

def norm_header(s: str) -> str:
    s = strip_accents((s or "").strip().upper())
    s = re.sub(r"\s+", " ", s)
    return s

def od_to_download(url: str) -> str:
    u = (url or "").strip()
    if not u.lower().startswith(("http://","https://")): u = "https://" + u
    if "sharepoint.com" in u:
        if "?web=1" in u: u = u.replace("?web=1","?download=1")
        elif "download=1" not in u: u = f"{u}{'&' if '?' in u else '?'}download=1"
    elif "onedrive.live.com" in u:
        u = u.replace("redir","download").replace("view.aspx","download.aspx")
        if "download=1" not in u: u = f"{u}{'&' if '?' in u else '?'}download=1"
    return u

# ======== Cache del DataFrame ========
_df_cache = None
_df_at = 0.0

def load_df(force: bool=False) -> pd.DataFrame:
    global _df_cache, _df_at
    now = time.time()
    if (not force) and _df_cache is not None and (now - _df_at) < CACHE_TTL:
        return _df_cache
    url = od_to_download(ONEDRIVE_URL)
    url = f"{url}{'&' if '?' in url else '?'}_cb={int(now)}"
    r = requests.get(url, timeout=60, headers={"Cache-Control":"no-cache"})
    r.raise_for_status()
    ct = r.headers.get("content-type","").lower()
    if ("html" in ct) or (not r.content[:2]==b"PK" and "spreadsheetml" not in ct):
        sample = r.text[:200].replace("\n"," ")
        raise RuntimeError(f"El vínculo de OneDrive no devolvió XLSX. Content-Type={ct}. Muestra: {sample}")
    df = pd.read_excel(io.BytesIO(r.content),
                       sheet_name=(SHEET_NAME if SHEET_NAME else 0),
                       engine="openpyxl",
                       header=HEADER_ROW-1)
    _df_cache, _df_at = df, now
    return df

# ======== Detección de columnas por encabezado ========
NAME_H = "NOMBRES Y APELLIDOS"
DOC_H  = "DNI / CE"
VIG_H  = "FECHA DE VIGENCIA DE HABILITACION DE LICENCIA INTERNA"
EST_H  = "ESTATUS DE PROCESO DE HABILITACION"

def detect_cols(df: pd.DataFrame):
    cols_norm = {i: norm_header(str(c)) for i,c in enumerate(df.columns)}
    name_idx = doc_idx = vig_idx = est_idx = None
    for i,h in cols_norm.items():
        if h == norm_header(NAME_H): name_idx = i
        if h == norm_header(DOC_H):  doc_idx  = i
        if h == norm_header(VIG_H):  vig_idx  = i
        if h == norm_header(EST_H):  est_idx  = i
    # Fallback (0-based): D=3, E=4, AF=31, AG=32
    name_idx = 3  if name_idx is None else name_idx
    doc_idx  = 4  if doc_idx  is None else doc_idx
    vig_idx  = 31 if vig_idx  is None else vig_idx
    est_idx  = 32 if est_idx  is None else est_idx
    return name_idx, doc_idx, vig_idx, est_idx

def val(row, idx):
    try:
        import pandas as pd
        v = row.iloc[idx]
        if pd.isna(v): return ""
        return str(v).strip()
    except:
        return ""

# ======== FastAPI ========
app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[ALLOWED_ORIGIN] if ALLOWED_ORIGIN!="*" else ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/", response_class=PlainTextResponse)
def health():
    return "OK"

@app.get("/driver", response_class=HTMLResponse)
def driver(doc: str = Query(...), t: str = Query(...), nocache: Optional[int] = 0):
    ndoc = normalize_doc(doc)
    mac  = hmac.new(SECRET_KEY, ndoc.encode("utf-8"), hashlib.sha256).digest()
    if t != b64url(mac):
        return Response("Token inválido", status_code=403)

    df = load_df(force=bool(nocache))
    name_idx, doc_idx, vig_idx, est_idx = detect_cols(df)

    matches = df[df.iloc[:, doc_idx].apply(lambda x: normalize_doc("" if pd.isna(x) else str(x))) == ndoc]
    if matches.empty:
        return Response("No se encontró al conductor.", status_code=404)

    row = matches.iloc[0]
    nombres = val(row, name_idx)
    dni_ce  = val(row, doc_idx)
    vig     = val(row, vig_idx)
    est     = val(row, est_idx)

    html = f"""
    <!doctype html><meta charset="utf-8">
    <style>
      :root {{ --bg:#0b0b0b; --card:#1f2937; --muted:#9ca3af; --fg:#e5e7eb; --pill:#111827; --pillbd:#334155; }}
      body{{margin:0;padding:20px;background:var(--bg);color:var(--fg);font-family:system-ui,-apple-system,Segoe UI,Roboto,Ubuntu,Arial,sans-serif}}
      .card{{max-width:720px;margin:0 auto;border-radius:16px;padding:24px;border:1px solid #374151;background:var(--card);box-shadow:0 2px 16px rgba(0,0,0,.25)}}
      h1{{margin:0 0 18px;font-size:28px;font-weight:800}}
      .row{{display:flex;justify-content:space-between;gap:16px;padding:12px 0;border-bottom:1px solid #374151}}
      .k{{color:var(--muted);letter-spacing:.5px}}
      .v{{font-weight:800;white-space:pre-line}}
      .pill{{display:inline-block;padding:6px 12px;border-radius:999px;border:1px solid var(--pillbd);background:var(--pill);font-weight:800}}
      .foot{{margin-top:14px;color:var(--muted);font-size:14px}}
    </style>
    <div class="card">
      <h1>Información del Conductor</h1>
      <div class="row"><div class="k">NOMBRES Y APELLIDOS</div><div class="v">{(nombres or '-').replace('\\n','<br>')}</div></div>
      <div class="row"><div class="k">DNI / CE</div><div class="v">{dni_ce or '-'}</div></div>
      <div class="row"><div class="k">FECHA DE VIGENCIA DE HABILITACIÓN DE LICENCIA INTERNA</div><div class="v">{vig or '-'}</div></div>
      <div class="row"><div class="k">ESTATUS DE PROCESO DE HABILITACION</div><div class="v"><span class="pill">{est or '-'}</span></div></div>
      <div class="foot">Fuente consultada en tiempo real. TTL caché: {CACHE_TTL}s. Fila encabezados: {HEADER_ROW}.</div>
    </div>
    """
    return HTMLResponse(html)
