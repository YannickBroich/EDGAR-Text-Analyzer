
import os, re, time, errno, pandas as pd, requests
from datetime import datetime

# --------- Config ---------
BASE_DIR   = r"C:\Users\Yannick\Downloads\EDGAR Text Analyzer"
CSV_PATH   = os.path.join(BASE_DIR, "Data", "sic_6331_insurers.csv")  # adjust to the sic file you want
SAVE_ROOT  = os.path.join(BASE_DIR, "Filings")                        
USER_AGENT = "exampleemail@email.com"                           # IMPORTANT
AFTER_DATE = "2010-01-01"                                       #Select date
FORMS_OK   = {"10-K", "10-KT"}

os.makedirs(SAVE_ROOT, exist_ok=True)
HEADERS = {"User-Agent": USER_AGENT, "Accept-Encoding": "gzip, deflate"}

# --------- Helpers ---------
def ensure_dir(p):
    try:
        os.makedirs(p, exist_ok=True)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

def clean_name(name: str) -> str:
    """Windows-safe, compact company folder name."""
    name = re.sub(r"[^\w\s\-.&]", "", str(name))      # drop odd chars
    name = re.sub(r"\s+", "_", name.strip())          # spaces -> _
    # optional trims: remove trailing punctuation/underscores
    name = name.strip("._-")
    return name[:80] if name else "Unknown_Company"

def cik_noleadzeros(cik_10: str) -> str:
    return str(int(cik_10))

def get_submissions_json(cik_10: str) -> dict:
    url = f"https://data.sec.gov/submissions/CIK{cik_10}.json"
    r = requests.get(url, headers=HEADERS, timeout=60)
    r.raise_for_status()
    return r.json()

def download_full_submission(cik_10: str, accession_dash: str, dest_folder: str) -> str:
    """Downloads the EDGAR full submission TXT for a given accession."""
    cik_clean = cik_noleadzeros(cik_10)
    acc_nodash = accession_dash.replace("-", "")
    url = f"https://www.sec.gov/Archives/edgar/data/{cik_clean}/{acc_nodash}/{accession_dash}.txt"
    dest_path = os.path.join(dest_folder, "full-submission.txt")
    if os.path.isfile(dest_path):
        return dest_path
    resp = requests.get(url, headers=HEADERS, timeout=120)
    resp.raise_for_status()
    with open(dest_path, "wb") as f:
        f.write(resp.content)
    return dest_path


def main():
    df = pd.read_csv(CSV_PATH)
    if "CIK" not in df.columns:
        raise RuntimeError("CSV needs a 'CIK' column.")
    df["CIK"] = df["CIK"].astype(str).str.replace(r"\D", "", regex=True).str.zfill(10)

    total = len(df)
    after_dt = datetime.strptime(AFTER_DATE, "%Y-%m-%d")

    for i, row in df.iterrows():
        cik10    = row["CIK"]
        company  = clean_name(row.get("Company", f"CIK_{cik10}"))
        print(f"\n[{i+1}/{total}] {company} ({cik10})")

        # company folder directly under SAVE_ROOT
        company_dir = os.path.join(SAVE_ROOT, company)
        ensure_dir(company_dir)

        # pull recent filings list
        try:
            data = get_submissions_json(cik10)
        except Exception as e:
            print("  Submissions JSON error:", e); time.sleep(0.4); continue

        recent = data.get("filings", {}).get("recent", {})
        forms  = recent.get("form", [])
        dates  = recent.get("filingDate", [])
        accs   = recent.get("accessionNumber", [])

        keep = []
        for form, fdate, acc in zip(forms, dates, accs):
            if form not in FORMS_OK:
                continue
            try:
                dt = datetime.strptime(fdate, "%Y-%m-%d")
            except Exception:
                continue
            if dt >= after_dt:
                keep.append((fdate, acc, form))

        if not keep:
            print("  No 10-K/10-KT ≥ 2010.")
            continue

        keep.sort(reverse=True)  # newest first
        downloaded = 0
        for fdate, acc, form in keep:
            acc_dir = os.path.join(company_dir, acc)
            ensure_dir(acc_dir)
            try:
                p = download_full_submission(cik10, acc, acc_dir)
                downloaded += 1
                print(f"   ✓ {form} {fdate} -> {p}")
            except Exception as e:
                print(f"   ✗ {form} {fdate} ({acc}) -> {e}")
            time.sleep(0.2)

        print(f"  Done: {downloaded}/{len(keep)} files.")

    print(f"\nFinished. Files are under:\n  {SAVE_ROOT}\\<CompanyName>\\<ACCESSION>\\full-submission.txt")

if __name__ == "__main__":
    main()
