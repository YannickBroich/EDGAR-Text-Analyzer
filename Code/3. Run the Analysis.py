

import os, re, glob, html as htmlmod, logging
from concurrent.futures import ProcessPoolExecutor, as_completed
import pandas as pd

# ---------------- Projekt-Root ----------------
BASE_DIR       = r"YourPath\EDGAR Text Analyzer"
DATA_DIR       = os.path.join(BASE_DIR, "Data")
FILINGS_NEW    = os.path.join(BASE_DIR, "Filings")             
FILINGS_LEGACY = os.path.join(BASE_DIR, "sec-edgar-filings")   
OUT_DIR        = os.path.join(BASE_DIR, "EDGAR_analysis")
CSV_MAP        = os.path.join(DATA_DIR, "sic_6331_insurers.csv")

os.makedirs(OUT_DIR, exist_ok=True)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.FileHandler(os.path.join(OUT_DIR, "edgar_analysis.log")), logging.StreamHandler()]
)
log = logging.getLogger("edgar")

#
DICT = {
    "A_core_policy": [
        r"\bmonetary policy\b", r"\bpolicy (?:stance|rate|rates)\b",
        r"\bpolicy (?:tightening|loosening|accommodative|restrictive|normalization)\b", r"\bFOMC\b"
    ],
    "B_rates_movement": [
        r"\binterest rate(?:s)?\b", r"\brate hike(?:s)?\b", r"\brate cut(?:s)?\b",
        r"\brising rates\b", r"\blower rates\b",
        r"\brate (?:increase|increases|decrease|decreases)\b",
        r"\bfederal funds rate\b", r"\bshort-?term rates\b", r"\blong-?term rates\b"
    ],
    "C_yield_duration": [
        r"\byield curve\b", r"\b(inverted|steepen(?:ed|ing)?|flatten(?:ed|ing)?) yield curve\b",
        r"\btreasury yields?\b", r"\bbond yields?\b",
        r"\bduration(?: gap)?\b", r"\bconvexity\b", r"\breinvestment (?:rate|yield)s?\b"
    ],
    "D_transmission": [
        r"\bcredit conditions\b", r"\bborrowing costs?\b", r"\bfunding costs?\b",
        r"\bcost of capital\b", r"\bdiscount rate\b"
    ],
    "F_institutions": [
        r"\bFederal Reserve\b|\bthe Fed\b", r"\bcentral bank(?:s)?\b", r"\bECB\b"
    ],
}
RX = {cat: [re.compile(p, re.I) for p in pats] for cat, pats in DICT.items()}


def fast_html_to_text(html_fragment: str) -> str:
    if not html_fragment:
        return ""
    txt = re.sub(r"(?is)<(script|style).*?</\1>", " ", html_fragment)
    txt = re.sub(r"(?is)<[^>]+>", " ", txt)
    txt = htmlmod.unescape(txt)
    return re.sub(r"\s+", " ", txt).strip()

def read_text_any(path):
    for enc in ("utf-8", "cp1252", "latin-1"):
        try:
            with open(path, "r", encoding=enc, errors="ignore") as f:
                return f.read()
        except Exception:
            pass
    return ""

def extract_10k_html_from_submission(txt: str) -> str:
    for blk in re.findall(r"(?is)<document>(.*?)</document>", txt):
        if re.search(r"(?is)<type>\s*10-\s*k", blk):
            m_text = re.search(r"(?is)<text>", blk)
            content = blk[m_text.end():] if m_text else blk
            m_html = re.search(r"(?is)<html", content)
            return content[m_html.start():] if m_html else content
    return ""

def extract_header_meta(txt: str) -> dict:
    meta = {}
    pats = [
        ("FILED_AS_OF_DATE", r"FILED AS OF DATE:\s*([0-9]{8})"),
        ("CONFORMED_PERIOD_OF_REPORT", r"CONFORMED PERIOD OF REPORT:\s*([0-9]{8})"),
        ("ACCESSION_NUMBER", r"ACCESSION NUMBER:\s*([0-9]{10}-[0-9]{2}-[0-9]{6})"),
        ("CENTRAL_INDEX_KEY", r"CENTRAL INDEX KEY:\s*([0-9]{1,10})"),
        ("COMPANY_CONFORMED_NAME", r"COMPANY CONFORMED NAME:\s*(.+)")
    ]
    for key, pat in pats:
        m = re.search(pat, txt)
        if m:
            meta[key] = m.group(1).strip()
    if "CENTRAL_INDEX_KEY" in meta:
        meta["CENTRAL_INDEX_KEY"] = meta["CENTRAL_INDEX_KEY"].zfill(10)
    return meta

def to_quarter(yyyymmdd: str) -> str:
    if not yyyymmdd or len(yyyymmdd) < 6 or not yyyymmdd[:6].isdigit():
        return ""
    y = yyyymmdd[:4]; m = int(yyyymmdd[4:6]); q = (m-1)//3 + 1
    return f"{y}Q{q}"

def load_cik_map(csv_path):
    try:
        df = pd.read_csv(csv_path)
        if "CIK" in df.columns and "Company" in df.columns:
            df["CIK"] = df["CIK"].astype(str).str.replace(r"\D", "", regex=True).str.zfill(10)
            return dict(zip(df["CIK"], df["Company"]))
    except Exception:
        pass
    return {}

def find_snippets(text: str, window=200):
    hits = []
    for cat, pats in RX.items():
        for rx in pats:
            for m in rx.finditer(text):
                s, e = m.start(), m.end()
                left, right = max(0, s-window), min(len(text), e+window)
                hits.append((cat, rx.pattern, text[left:right]))
    return hits


def collect_full_submission_paths() -> list[str]:
    paths = []
    
    if os.path.isdir(FILINGS_NEW):
        paths += glob.glob(os.path.join(FILINGS_NEW, "**", "full-submission*"), recursive=True)
    
    if os.path.isdir(FILINGS_LEGACY):
        paths += glob.glob(os.path.join(FILINGS_LEGACY, "*", "10-K", "*", "full-submission*"))
    return sorted(set(paths))

def process_filing_folder(folder: str, cik_to_company: dict) -> list[dict]:
    try:
        sub_paths = glob.glob(os.path.join(folder, "full-submission*"))
        if not sub_paths:
            return []
        sub_txt = read_text_any(sub_paths[0])
        if not sub_txt:
            return []

        meta     = extract_header_meta(sub_txt)
        filed    = meta.get("FILED_AS_OF_DATE", "")
        period   = meta.get("CONFORMED_PERIOD_OF_REPORT", "")
        quarter  = to_quarter(filed) if filed else ""
        accession= meta.get("ACCESSION_NUMBER", "") or os.path.basename(folder)
        cik      = meta.get("CENTRAL_INDEX_KEY", "")
        company  = (meta.get("COMPANY_CONFORMED_NAME", "").strip()
                    or os.path.basename(os.path.dirname(folder))
                    or cik_to_company.get(cik, f"CIK_{cik}" if cik else "UNKNOWN"))

        html_fragment = extract_10k_html_from_submission(sub_txt)
        text = fast_html_to_text(html_fragment)
        if not text:
            html_files = glob.glob(os.path.join(folder, "*.htm")) + glob.glob(os.path.join(folder, "*.html"))
            if html_files:
                html_files.sort(key=lambda p: os.path.getsize(p), reverse=True)
                text = fast_html_to_text(read_text_any(html_files[0]))
        if not text:
            return []

        out = []
        for cat, term, snip in find_snippets(text, window=200):
            out.append({
                "company": company, "cik": cik,
                "filing_date": filed, "period": period, "quarter": quarter,
                "accession": accession, "category": cat, "term_regex": term,
                "snippet": snip[:1000], "folder": folder
            })
        return out
    except Exception as e:
        log.warning(f"Error {folder}: {e}")
        return []


if __name__ == "__main__":
    log.info(f"Project-Root = {BASE_DIR}")

    cik_to_company = load_cik_map(CSV_MAP)
    fullsubs = collect_full_submission_paths()
    targets  = sorted({os.path.dirname(p) for p in fullsubs})
    log.info(f"Found: {len(targets)} Filing-Folder")

    MAX_WORKERS = max(1, os.cpu_count() - 1)
    SAMPLE_N    = None
    targets     = targets if SAMPLE_N is None else targets[:SAMPLE_N]

    detailed_rows = []
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(process_filing_folder, folder, cik_to_company): folder for folder in targets}
        done = 0
        for fut in as_completed(futures):
            res = fut.result()
            if res:
                detailed_rows.extend(res)
            done += 1
            if done % 20 == 0 or done == len(futures):
                log.info(f"Progress: {done}/{len(futures)}")

    det_df = pd.DataFrame(detailed_rows)
    det_out = os.path.join(OUT_DIR, "monpol_hits_detailed.csv")
    det_df.to_csv(det_out, index=False, encoding="utf-8")
    log.info(f"Detailed -> {det_out} (rows={len(det_df)})")

    if det_df.empty:
        cnt_df = pd.DataFrame(columns=["company","cik","quarter","category","term_regex","hits"])
    else:
        cnt_df = (det_df.groupby(["company","cik","quarter","category","term_regex"], as_index=False)
                        .size().rename(columns={"size":"hits"}))
    cnt_out = os.path.join(OUT_DIR, "monpol_hits_counts.csv")
    cnt_df.to_csv(cnt_out, index=False, encoding="utf-8")
    log.info(f"Counts   -> {cnt_out} (rows={len(cnt_df)})")
