import requests, xml.etree.ElementTree as ET, pandas as pd, re, time, os

USER_AGENT = "your-email@example.com"    
PAGE_SIZE  = 100                          
TIMEOUT    = 30

def get_companies_by_sic_all(sic: str, page_size: int = PAGE_SIZE):
    ns = "{http://www.w3.org/2005/Atom}"
    headers = {"User-Agent": USER_AGENT}
    start = 0
    rows, seen = [], set()

    while True:
        url = (f"https://www.sec.gov/cgi-bin/browse-edgar?"
               f"action=getcompany&SIC={sic}&owner=exclude&count={page_size}"
               f"&start={start}&output=atom")
        r = requests.get(url, headers=headers, timeout=TIMEOUT)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        entries = root.findall(f"{ns}entry")
        if not entries:
            break

        new_cnt = 0
        for e in entries:
            title = (e.find(f"{ns}title").text or "").strip()
            idtxt = e.find(f"{ns}id").text or ""
            m = re.search(r"CIK=(\d+)", idtxt, flags=re.I)
            if not m: 
                continue
            cik = re.sub(r"\D", "", m.group(1)).zfill(10)
            if cik == "0000000000" or cik in seen:
                continue
            seen.add(cik)
            rows.append({"Company": title, "CIK": cik})
            new_cnt += 1

        
        print(f"Fetched +{new_cnt} (total {len(rows)}) at start={start}")
        start += page_size
        time.sleep(0.2)  # to avoid getting blocked

    return pd.DataFrame(rows)

#Chose relevant SIC here
SIC = "6331"  # 

df = get_companies_by_sic_all(SIC)
df = df.drop_duplicates(subset=["CIK"]).sort_values("Company").reset_index(drop=True)


SAVE_DIR = r"YourPath\EDGAR Text Analyzer\Data"
os.makedirs(SAVE_DIR, exist_ok=True)  


out_csv = os.path.join(SAVE_DIR, f"sic_{SIC}_insurers.csv")


df.to_csv(out_csv, index=False, encoding="utf-8")

