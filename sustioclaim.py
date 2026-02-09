import io, re, calendar
from datetime import date
import numpy as np
import pandas as pd
import streamlit as st
from difflib import SequenceMatcher

st.set_page_config(page_title="Sustio Claim Builder", layout="wide")
st.title("📊 Sustio Claim Builder")

st.markdown(
    "- Attendance codes: **M, N, M8, N8, RN8, RM8, ON8, PM8, PN8**\n"
    "- Claim window = **16th of previous month → 15th of selected month** (choose below)\n"
    "- **Transportation** and **Shift** are ignored\n"
    "- Matching order: **Worker No (raw & digits)** → **Name (exact)** → **Name (fuzzy)**\n"
    "- **Eligibility cap**: days **outside** [`JOIN_DATE`, `JOIN_DATE+3 months−1 day`] are set to **0**"
)

# ───────────────────────────── Constants ─────────────────────────────
BASE_PRESENT = {"M","N","M8","N8","RN8","RM8","ON8","PM8","PN8","PN","PM","RN","RM","MR","NR","NR8","MR8","ON","OM8"}
MARK_8H      = {"M8","N8","RN8","RM8","ON8","PM8","PN8","OM8"}

# ───────────────────────────── Helpers ─────────────────────────────

def excel_engine_from_name(name: str):
    name = str(name).lower()
    return "openpyxl" if name.endswith("xlsx") else "xlrd"

def norm_text(s: pd.Series) -> pd.Series:
    return s.astype(str).str.replace("\u00a0"," ", regex=False).str.strip()

def clean_code(v) -> str:
    """Strict: only exact present codes count; MC/AL/etc -> absent."""
    if pd.isna(v): return ""
    s = str(v).strip().upper()
    if s in {"OM","0M"}: s = "M"
    elif s in {"ON","0N"}: s = "N"
    return s if s in BASE_PRESENT else ""

def reorder_day_cols(cols):
    nums = []
    for c in cols:
        try:
            n = int(str(c).strip())
            if 1 <= n <= 31: nums.append(n)
        except: pass
    nums = sorted(set(nums))
    return [str(d) for d in range(16,32) if d in nums] + [str(d) for d in range(1,16) if d in nums]

def make_keys(series: pd.Series):
    raw   = norm_text(series).str.upper()
    digit = raw.str.replace(r"\D","", regex=True).str.lstrip("0")
    return raw, digit

def normalize_name(s: pd.Series) -> pd.Series:
    return (
        s.astype(str)
         .str.upper()
         .str.replace(r"[^A-Z0-9 ]"," ", regex=True)
         .str.replace(r"\s+"," ", regex=True)
         .str.strip()
    )


def parse_join_date(series_like) -> pd.Series:
    """Robust JOIN_DATE parser:
    - blanks/None/'0'/0 -> NaT
    - normal text dates (day-first) -> datetime
    - Excel serial numbers -> datetime (origin 1899-12-30)
    """
    s = pd.Series(series_like).copy()
    s = s.replace({0: np.nan, 0.0: np.nan, "0": np.nan, "0000-00-00": np.nan,
                   "NONE": np.nan, "NaN": np.nan, "nan": np.nan})
    dt = pd.to_datetime(s, errors="coerce", dayfirst=True)
    num = pd.to_numeric(s, errors="coerce")
    serial_mask = dt.isna() & num.notna()
    if serial_mask.any():
        dt.loc[serial_mask] = pd.to_datetime(num.loc[serial_mask], unit="D", origin="1899-12-30")
    bad_mask = dt.notna() & (dt.dt.year < 1990)
    dt.loc[bad_mask] = pd.NaT
    return dt  

# Eligible End Date helper (JOIN_DATE + 3 months − 1 day)
def add_eligible_end_date(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "JOIN_DATE" in out.columns:
        eligible = pd.to_datetime(out["JOIN_DATE"], errors="coerce") + pd.offsets.DateOffset(months=3) - pd.Timedelta(days=1)
        eligible = eligible.where(~out["JOIN_DATE"].isna())
        insert_at = out.columns.get_loc("JOIN_DATE") + 1
        out.insert(insert_at, "Eligible End Date", eligible)
    return out

# Cap days outside [JOIN_DATE .. Eligible End] within the real calendar claim window
def cap_days_by_window(df: pd.DataFrame, day_cols, cycle_end):
    """
    Zero out attendance outside [JOIN_DATE .. Eligible End] using real calendar dates
    for the claim window (16th of previous month -> 15th of selected month).
    """
    d = df.copy()

    # Window bounds
    cycle_end_ts   = pd.Timestamp(cycle_end.year, cycle_end.month, 15)
    cycle_start_dt = cycle_end_ts - pd.offsets.MonthBegin(1)  # 1st of end-month
    prev_month_end = cycle_start_dt - pd.Timedelta(days=1)    # last day of previous month
    window_start   = pd.Timestamp(prev_month_end.year, prev_month_end.month, 16)

    # Per-row limits
    join = pd.to_datetime(d["JOIN_DATE"], errors="coerce")
    elig = join + pd.offsets.DateOffset(months=3) - pd.Timedelta(days=1)

    prev_month = window_start.month
    prev_year  = window_start.year
    end_month  = cycle_end_ts.month
    end_year   = cycle_end_ts.year

    for c in day_cols:
        day = int(c)
        # First segment: 16..31 in previous month
        if 16 <= day <= 31:
            try:
                actual = pd.Timestamp(prev_year, prev_month, day)
            except ValueError:
                if c in d.columns: d[c] = 0
                continue
        else:
            # Second segment: 1..15 in selected (end) month
            try:
                actual = pd.Timestamp(end_year, end_month, day)
            except ValueError:
                if c in d.columns: d[c] = 0
                continue

        col = str(c)
        if col not in d.columns:
            continue

        # Zero if actual date is before JOIN or after Eligible End (per row)
        mask_before = join.notna() & (join > actual)
        mask_after  = elig.notna() & (elig < actual)
        d.loc[mask_before | mask_after, col] = 0

    d["Total Working Days"] = d[day_cols].sum(axis=1).astype(int)
    return d

# ───────────────── Attendance parsing ─────────────────
def detect_attendance(att_file):
    engine = excel_engine_from_name(att_file.name)
    raw = pd.read_excel(att_file, sheet_name=0, header=None, engine=engine)
    header_idx = 0; found_emp=False; found_name=False
    for i in range(min(25, len(raw))):
        vals = norm_text(raw.iloc[i]).str.lower().tolist()
        has_emp = any(v in {"emp no","employee no","worker no","emp id","employee id","no pekerja","id"} for v in vals)
        has_nam = any(v in {"name","worker name","employee name","nama"} for v in vals)
        if has_emp and has_nam:
            header_idx = i; found_emp=True; found_name=True; break

    day_row_idx = None
    for j in range(header_idx+1, min(header_idx+8, len(raw))):
        vals = norm_text(raw.iloc[j])
        if vals.str.fullmatch(r"\d{1,2}").sum() >= 6:
            day_row_idx = j; break

    if day_row_idx is not None:
        df = pd.read_excel(
    att_file,
    sheet_name=0,
    header=[header_idx, day_row_idx],
    engine=engine,
)

        flat = []
        for t,b in df.columns:
            ts, bs = str(t).strip(), str(b).strip()
            if re.fullmatch(r"\d{1,2}", bs): flat.append(bs)
            elif re.fullmatch(r"\d{1,2}", ts): flat.append(ts)
            else: flat.append(ts if ts and ts.lower()!="nan" else bs)
        df.columns = flat
    else:
        df = pd.read_excel(
    att_file,
    sheet_name=0,
    header=header_idx,
    engine=engine,
)


    df = df.dropna(how="all").reset_index(drop=True)
    df.columns = norm_text(pd.Index(df.columns))

    ren = {}
    for c in df.columns:
        cl = c.lower()
        if cl in {"emp no","employee no","worker no","emp id","employee id","no pekerja","id"}: ren[c] = "Worker No"
        elif cl in {"name","worker name","employee name","nama"}: ren[c] = "Worker Name"
        elif cl in {"joined date","join date","date join","date joined","tarikh masuk","doj"}: ren[c] = "JOIN_DATE"
        elif cl in {"transportation","transport"}: ren[c] = "_drop_transport"
        elif cl in {"shift"}: ren[c] = "_drop_shift"
    if ren: df = df.rename(columns=ren)
    df = df[[c for c in df.columns if not str(c).startswith("_drop_")]]

    conv = {}
    for c in df.columns:
        try:
            iv = int(str(c))
            if 1 <= iv <= 31: conv[c] = str(iv)
        except: pass
    if conv: df = df.rename(columns=conv)

    for col in ["Worker No","Worker Name","JOIN_DATE"]:
        if col not in df.columns: df[col] = pd.NA

    day_cols = [c for c in df.columns if str(c).isdigit()]
    day_cols = reorder_day_cols(day_cols)
    return df, day_cols, dict(header_idx=header_idx, day_row_idx=day_row_idx,
                              found_emp=found_emp, found_name=found_name)

def build_presence(att_df: pd.DataFrame, day_cols):
    d = att_df.copy()
    d["Worker No"]   = norm_text(d["Worker No"]).str.upper()
    d["Worker Name"] = norm_text(d["Worker Name"])
    d["NAME_KEY"]    = normalize_name(d["Worker Name"])
    d["WORKER_NO_KEY_RAW"], d["WORKER_NO_KEY_DIGIT"] = make_keys(d["Worker No"])

    has_8h = pd.Series(False, index=d.index)
    for c in day_cols:
        col = d[c].apply(clean_code)
        d[c] = col.isin(BASE_PRESENT).astype(int)
        has_8h = has_8h | col.isin(MARK_8H)

    d["Worker_Type"] = np.where(has_8h, "8H", "12H")
    d["Total Working Days"] = d[day_cols].sum(axis=1).astype(int)
    return d

# ───────────────── Masterlist normalization (patched) ─────────────────
def normalize_masterlist_auto(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d.columns = norm_text(pd.Index(d.columns))

    ren = {}
    for c in d.columns:
        cl = c.lower()
        if cl in {
            "emp id","empid","employee id","employeeid",
            "worker id","worker no","emp no","employee no",
            "no pekerja","no.pekerja","id","emp id no","employee id no"
        }:
            ren[c] = "Worker No"
        elif cl in {"name","worker name","employee name","nama","nama pekerja"}:
            ren[c] = "Worker Name"
        elif cl in {"join date","joined date","date joined","date of join","doj","tarikh masuk","date joined"}:
            ren[c] = "JOIN_DATE"
        elif cl in {"recruiter","recruiter name","consultant","agent","pic","pic recruiter","consultant name","recuiter"}:
            ren[c] = "Recruiter"
    d = d.rename(columns=ren)
    return d

def apply_masterlist_mapping(raw_df: pd.DataFrame, map_cols: dict) -> pd.DataFrame:
    d = raw_df.copy()
    for std, sel in map_cols.items():
        d[std] = d[sel] if sel in d.columns else pd.NA

    d["Worker No"]   = norm_text(d["Worker No"]).str.upper()
    d["Worker Name"] = norm_text(d["Worker Name"])
    d["JOIN_DATE"]   = parse_join_date(d["JOIN_DATE"])
    d["Recruiter"]   = norm_text(d["Recruiter"]).replace({"": pd.NA}) if "Recruiter" in d.columns else pd.NA
    d["NAME_KEY"]    = normalize_name(d["Worker Name"])
    d["WORKER_NO_KEY_RAW"], d["WORKER_NO_KEY_DIGIT"] = make_keys(d["Worker No"])

    keep = ["Worker No","Worker Name","JOIN_DATE","Recruiter",
            "NAME_KEY","WORKER_NO_KEY_RAW","WORKER_NO_KEY_DIGIT"]
    for k in keep:
        if k not in d.columns: d[k] = pd.NA
    return d[keep].copy()

# ───────────────── Matching & enrichment (with fuzzy fallback) ─────────────────
def enrich_and_filter(pres_df: pd.DataFrame, master: pd.DataFrame, day_cols,
                      keep_only_master=True, fuzzy_threshold=0.90):
    df = pres_df.copy()

    valid_raw   = set(master["WORKER_NO_KEY_RAW"].dropna().astype(str))
    valid_digit = set(master["WORKER_NO_KEY_DIGIT"].dropna().astype(str))
    valid_name  = set(master["NAME_KEY"].dropna().astype(str))

    mask = df["WORKER_NO_KEY_RAW"].isin(valid_raw) | \
           df["WORKER_NO_KEY_DIGIT"].isin(valid_digit) | \
           df["NAME_KEY"].isin(valid_name)

    unmatched = df[~mask].copy()
    if keep_only_master:
        df = df[mask].reset_index(drop=True)
    else:
        df = df.reset_index(drop=True)

    lut_raw_rec   = master.drop_duplicates("WORKER_NO_KEY_RAW").set_index("WORKER_NO_KEY_RAW")["Recruiter"]
    lut_dig_rec   = master.drop_duplicates("WORKER_NO_KEY_DIGIT").set_index("WORKER_NO_KEY_DIGIT")["Recruiter"]
    lut_name_rec  = master.drop_duplicates("NAME_KEY").set_index("NAME_KEY")["Recruiter"]

    lut_raw_jdt   = master.drop_duplicates("WORKER_NO_KEY_RAW").set_index("WORKER_NO_KEY_RAW")["JOIN_DATE"]
    lut_dig_jdt   = master.drop_duplicates("WORKER_NO_KEY_DIGIT").set_index("WORKER_NO_KEY_DIGIT")["JOIN_DATE"]
    lut_name_jdt  = master.drop_duplicates("NAME_KEY").set_index("NAME_KEY")["JOIN_DATE"]

    rec = df["WORKER_NO_KEY_RAW"].map(lut_raw_rec)\
            .fillna(df["WORKER_NO_KEY_DIGIT"].map(lut_dig_rec))\
            .fillna(df["NAME_KEY"].map(lut_name_rec))
    jdt = df["WORKER_NO_KEY_RAW"].map(lut_raw_jdt)\
            .fillna(df["WORKER_NO_KEY_DIGIT"].map(lut_dig_jdt))\
            .fillna(df["NAME_KEY"].map(lut_name_jdt))

    # fuzzy fallback
    need_fuzzy = rec.isna()
    if need_fuzzy.any() and len(master):
        ml_names = master["NAME_KEY"].dropna().unique().tolist()
        ml_rec   = master.drop_duplicates("NAME_KEY").set_index("NAME_KEY")["Recruiter"]
        ml_jdt   = master.drop_duplicates("NAME_KEY").set_index("NAME_KEY")["JOIN_DATE"]

        def best_match(name):
            if not isinstance(name, str) or not name: return (None, None)
            best_ratio = 0.0; best_key = None
            for mk in ml_names:
                r = SequenceMatcher(a=name, b=mk).ratio()
                if r > best_ratio:
                    best_ratio = r; best_key = mk
            if best_ratio >= fuzzy_threshold and best_key is not None:
                return (ml_rec.get(best_key, pd.NA), ml_jdt.get(best_key, pd.NaT))
            return (None, None)

        matched = df.loc[need_fuzzy, "NAME_KEY"].apply(best_match)
        rec.loc[need_fuzzy] = [t[0] for t in matched]
        jdt.loc[need_fuzzy] = [t[1] for t in matched]

    df["Recruiter"] = rec
    df["JOIN_DATE"] = jdt

    cols = ["Worker No","Worker Name","JOIN_DATE","Recruiter"] + day_cols + ["Total Working Days","Worker_Type"]
    return df[cols], unmatched

def recruiter_summary(df_all: pd.DataFrame, day_cols, rate=3):
    if df_all.empty:
        return pd.DataFrame([{"Recruiter":"TOTAL","Days":0,"Rate (RM)":rate,"Amount (RM)":0}])
    tmp = df_all.copy()
    tmp["Days"] = tmp[day_cols].sum(axis=1).astype(int)
    tmp["Recruiter"] = tmp["Recruiter"].fillna("Unassigned")
    grp = tmp.groupby("Recruiter", dropna=False)["Days"].sum().reset_index()
    grp["Rate (RM)"] = rate
    grp["Amount (RM)"] = grp["Days"] * rate
    total = pd.DataFrame([{
        "Recruiter":"TOTAL",
        "Days": int(grp["Days"].sum()),
        "Rate (RM)": rate,
        "Amount (RM)": int(grp["Amount (RM)"].sum())
    }])
    return pd.concat([grp.sort_values("Days", ascending=False).reset_index(drop=True), total], ignore_index=True)

# ───────────────────────────── UI ─────────────────────────────
st.sidebar.header("⚙️ Options")
rate_rm = st.sidebar.number_input("Per-day rate (RM)", min_value=0, value=3, step=1)
debug_mode = st.sidebar.checkbox("Debug / show parsing details", value=True) 
bypass_master = st.sidebar.checkbox("Bypass Masterlist filtering (show all attendance rows)", value=False)
fuzzy_thr = st.sidebar.slider("Fuzzy name match threshold", min_value=0.70, max_value=0.99, value=0.90, step=0.01)
exclude_unassigned_export = st.sidebar.checkbox("Exclude 'Unassigned' from download (Claim & Summary)", value=True)

# Month picker for ANY month (past/future): window = 16th prev → 15th selected
def month_year_picker(label="Claim month (ends on 15th)"):
    today = date.today()
    years = [today.year - 1, today.year, today.year + 1]
    months = list(range(1, 13))
    sel_year  = st.sidebar.selectbox(f"{label} — Year", years, index=years.index(today.year))
    sel_month = st.sidebar.selectbox(f"{label} — Month", [calendar.month_abbr[m] for m in months],
                                     index=today.month - 1)
    sel_month_num = months[[calendar.month_abbr[m] for m in months].index(sel_month)]
    return date(sel_year, sel_month_num, 15)

cycle_end = month_year_picker()

# Nice banner so the active window is clear
cycle_end_ts   = pd.Timestamp(cycle_end.year, cycle_end.month, 15)
cycle_start_dt = cycle_end_ts - pd.offsets.MonthBegin(1)
prev_month_end = cycle_start_dt - pd.Timedelta(days=1)
window_start   = pd.Timestamp(prev_month_end.year, prev_month_end.month, 16)
st.info(f"Claim window: **{window_start.date()} → {cycle_end_ts.date()}**")

att_file = st.file_uploader("📄 Attendance (xlsx/xls)", type=["xlsx","xls"])
ml_file  = st.file_uploader("📇 Masterlist (xlsx/xls)", type=["xlsx","xls"])

if att_file and ml_file:
    with st.spinner("Processing..."):
        # Attendance
        att_df, day_cols, meta = detect_attendance(att_file)

        # Masterlist (auto-normalize + mapping UI)
        ml_engine = excel_engine_from_name(ml_file.name)
        xls = pd.ExcelFile(ml_file, engine=ml_engine)
        ml_sheet = st.selectbox("Masterlist sheet", xls.sheet_names, index=0)
        master_raw = pd.read_excel(xls, sheet_name=ml_sheet, engine=ml_engine)
        auto_ml = normalize_masterlist_auto(master_raw)

        st.markdown("**Masterlist column mapping (adjust only if auto is wrong):**")
        cols_list = list(auto_ml.columns) or ["— no columns —"]

        def guess(cands, default=None):
            for c in cands:
                if c in cols_list: return c
            return default if default in cols_list else cols_list[0]

        sel_worker_no = st.selectbox("→ Worker No column", cols_list, index=cols_list.index(guess(["Worker No","Emp id","Emp ID","EMP ID"])) if cols_list else 0)
        sel_worker_nm = st.selectbox("→ Worker Name column", cols_list, index=cols_list.index(guess(["Worker Name","Name"])) if cols_list else 0)
        sel_join_date = st.selectbox("→ JOIN_DATE column", cols_list, index=cols_list.index(guess(["JOIN_DATE","Date joined","Joined Date"])) if cols_list else 0)
        sel_recruiter = st.selectbox("→ Recruiter column", cols_list, index=cols_list.index(guess(["Recruiter","Recuiter","Recruiter Name"])) if cols_list else 0)

        masterlist = apply_masterlist_mapping(
            auto_ml,
            {"Worker No": sel_worker_no, "Worker Name": sel_worker_nm,
             "JOIN_DATE": sel_join_date, "Recruiter": sel_recruiter}
        )

        # Build presence & match
        pres = build_presence(att_df, day_cols) if day_cols else pd.DataFrame()
        claim_all, unmatched = enrich_and_filter(
            pres, masterlist, day_cols,
            keep_only_master=(not bypass_master),
            fuzzy_threshold=fuzzy_thr
        )

        # Cap days by real dates in the selected cycle window, then add Eligible End to preview
        claim_all = cap_days_by_window(claim_all, day_cols, cycle_end)
        claim_all = add_eligible_end_date(claim_all)

        # Diagnostics
        if debug_mode:
            with st.expander("📇 Masterlist diagnostics", expanded=False):
                st.write(f"Total rows in masterlist: **{len(masterlist)}**")
                nn = masterlist[["Worker No","Worker Name","JOIN_DATE","Recruiter"]].notna().sum()
                st.write(f"Non-null → Worker No: **{nn['Worker No']}**, Worker Name: **{nn['Worker Name']}**, JOIN_DATE: **{nn['JOIN_DATE']}**, Recruiter: **{nn['Recruiter']}**")
                st.dataframe(masterlist.head(25), use_container_width=True)

            with st.expander("🔎 Attendance parsing diagnostics", expanded=False):
                st.write(f"Detected header row: **{meta['header_idx']}**  |  Day header row: **{meta['day_row_idx']}**")
                st.write(f"Day columns ({len(day_cols)}): {day_cols}")
                st.write(f"Attendance rows (raw): **{len(att_df)}**")

                # ⬇️ Adjustable attendance preview size
                preview_n = st.slider("How many attendance rows to preview?", 5, 200, 50, 5)
                st.dataframe(att_df.head(preview_n), use_container_width=True)

        # On-screen preview (combined; shows Eligible End Date)
        st.subheader(f"Claim — preview ({len(claim_all)})")
        preview_cols = ["Worker No","Worker Name","JOIN_DATE","Eligible End Date","Recruiter"] + day_cols + ["Total Working Days","Worker_Type"]
        st.dataframe(claim_all[preview_cols].head(50), use_container_width=True)

        st.subheader("👥 Per-Recruiter Summary")
        rec_sum = recruiter_summary(claim_all.drop(columns=["Worker_Type"], errors="ignore"), day_cols, rate=rate_rm)
        st.dataframe(rec_sum, use_container_width=True)

        # ---------- Build export (optionally excluding Unassigned) ----------
        if exclude_unassigned_export:
            excl_mask = claim_all["Recruiter"].isna() | (claim_all["Recruiter"].astype(str).str.strip() == "") | (claim_all["Recruiter"] == "Unassigned")
            claim_export = claim_all[~excl_mask].copy()
        else:
            claim_export = claim_all.copy()

        rec_sum_export = recruiter_summary(claim_export.drop(columns=["Worker_Type"], errors="ignore"), day_cols, rate=rate_rm)
        if exclude_unassigned_export and not rec_sum_export.empty:
            body = rec_sum_export[(rec_sum_export["Recruiter"] != "Unassigned") & (rec_sum_export["Recruiter"] != "TOTAL")].copy()
            total_row = pd.DataFrame([{
                "Recruiter": "TOTAL",
                "Days": int(body["Days"].sum()) if len(body) else 0,
                "Rate (RM)": int(body["Rate (RM)"].iloc[0]) if len(body) else rate_rm,
                "Amount (RM)": int(body["Amount (RM)"].sum()) if len(body) else 0,
            }])
            rec_sum_export = pd.concat([body, total_row], ignore_index=True)

        # ---------- Write Excel (single combined sheet) ----------
        out = io.BytesIO()
        with pd.ExcelWriter(out, engine="xlsxwriter") as wr:
            ordered_cols = ["Worker No","Worker Name","JOIN_DATE","Eligible End Date","Recruiter"] + day_cols + ["Total Working Days","Worker_Type"]
            ordered_cols = [c for c in ordered_cols if c in claim_export.columns]
            claim_export[ordered_cols].to_excel(wr, index=False, sheet_name="Claim")
            rec_sum_export.to_excel(wr, index=False, sheet_name="Recruiter_Summary")

        st.download_button(
            f"⬇️ Download Claim Report (Combined + Recruiter Summary){' — Unassigned Excluded' if exclude_unassigned_export else ''}",
            data=out.getvalue(),
            file_name="sustio_claim_report_project_one.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        # Optional unmatched download
        if debug_mode and len(unmatched):
            buf = io.BytesIO()
            unmatched.to_excel(buf, index=False, sheet_name="Unmatched")  
            st.download_button("⬇️ Download unmatched list", buf.getvalue(),
                file_name="unmatched_attendance.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

else:
    st.info("Upload both files to generate the claim report.")
