# sih_extract.py
import os
from datetime import datetime
import pandas as pd
from dbfread import DBF
import subprocess
import pyreaddbc.readdbc as readdbc

# --- CONFIGURE PATHS ---
BASE_DIR = os.path.dirname(__file__)
DADOS_DIR = os.path.join(BASE_DIR, "Dados")
CNV_DIR = os.path.join(BASE_DIR, "CNV")         # where PROC.CNV, ESTAB.CNV, MUNIC.CNV are
OUT_DIR = os.path.join(BASE_DIR, "OUTPUT")      # output folder
os.makedirs(OUT_DIR, exist_ok=True)

# --- DETECT CURRENT YEAR FILES ---
current_year = datetime.now().year
dbc_files = [
    os.path.join(DADOS_DIR, f)
    for f in os.listdir(DADOS_DIR)
    if f.lower().endswith(".dbc") and f[4:6] == str(current_year)[2:]
]

if not dbc_files:
    raise SystemExit(f"⚠️ No .dbc files for {current_year} found in {DADOS_DIR}")

print(f"📂 Found {len(dbc_files)} .dbc files for {current_year}:")
for f in dbc_files:
    print("  ", os.path.basename(f))


# --- FUNCTION TO READ CNV FILES ---
def read_cnv(path, code_len=10, code_from_end=False):
    mapping = {}
    if not os.path.exists(path):
        return mapping
    with open(path, encoding='latin-1', errors='ignore') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            # pega o código correto do final da linha se necessário
            if code_from_end:
                parts = line.split()
                code = parts[-1].strip()     # último elemento é o código real
                name = " ".join(parts[:-1]).strip()
            else:
                code = line[:code_len].strip()
                name = line[code_len:].strip()
            # remove código repetido no final se estiver lá
            if name.endswith(code):
                name = name[:-len(code)].strip()
            mapping[code] = name
    return mapping


# --- READ CNV / CNES FILES AND CREATE MAPPING ---
proc_files = [f for f in os.listdir(CNV_DIR) if f.upper().startswith("CIDX") or "PROC" in f.upper()] #PROCOBS2b.CNV

# agora lê todos os arquivos .dbc dentro da pasta CNES
CNES_DIR = os.path.join(BASE_DIR, "CNES")
estab_files = [f for f in os.listdir(CNES_DIR) if f.lower().endswith(".dbc")]

mun_files = [f for f in os.listdir(CNV_DIR) if "MICIBGE" in f.upper() or "MUNIC" in f.upper()]

proc_map = {}
for f in proc_files:
    proc_map.update(read_cnv(os.path.join(CNV_DIR, f), code_len=10, code_from_end=True))

estab_map = {}
target_cnes = ["6697054", "2118513"]

for f in estab_files:
    dbf_path = os.path.join(CNES_DIR, f[:-4] + ".dbf")
    if not os.path.exists(dbf_path):
        readdbc.dbc2dbf(os.path.join(CNES_DIR, f), dbf_path)
    
    dbf = DBF(dbf_path, encoding="latin-1")
    print(f"\n🔹 Columns in {os.path.basename(dbf_path)}:")
    print(dbf.field_names)

    found = False
    for rec in dbf:
        cnes_code = str(rec["CNES"]).strip()
        if cnes_code in target_cnes:
            found = True

            # find hospital/establishment name
            name_field = next(
                (k for k in rec.keys() if "NOME" in k.upper() or "RAZAO" in k.upper()),
                None
            )
            if name_field:
                estab_map[cnes_code] = rec[name_field].strip()
            else:
                estab_map[cnes_code] = f"CNES_{cnes_code}"

            print(f"✅ Found CNES {cnes_code}: {estab_map[cnes_code]}")

    if not found:
        print(f"❌ None of target CNES found in {os.path.basename(dbf_path)}")


mun_map = {}
for f in mun_files:
    mun_map.update(read_cnv(os.path.join(CNV_DIR, f), code_len=6, code_from_end=True))



# --- LOGS PARA VERIFICAR ---
print("\n🔹 PROC map sample:")
for k, v in list(proc_map.items())[:10]:
    print(f"{k} -> {v}")

print("\n🔹 ESTAB map sample:")
for k, v in list(estab_map.items())[:10]:
    print(f"{k} -> {v}")

print("\n🔹 MUN map sample:")
for k, v in list(mun_map.items())[:10]:
    print(f"{k} -> {v}")


# --- LOOP THROUGH ALL .DBC FILES ---
all_dfs = []

def convert_dbc_to_dbf(dbc_path, out_path):
    print(f"🗜️ Converting {os.path.basename(dbc_path)} → {os.path.basename(out_path)}")
    try:
        readdbc.dbc2dbf(dbc_path, out_path)
        print("✅ Conversion OK (pyreaddbc)")
        return True
    except Exception as e:
        print("❌ Conversion failed:", e)
        return False


for dbc_path in dbc_files:
    dbf_path = dbc_path[:-4] + ".dbf"

    # ✅ convert if not already converted
    if not os.path.exists(dbf_path):
        if not convert_dbc_to_dbf(dbc_path, dbf_path):
            continue

    print("📖 Reading:", os.path.basename(dbf_path))
    try:
        records = list(DBF(dbf_path, encoding="latin-1"))
    except Exception as e:
        print(f"❌ Failed to read {dbf_path}:", e)
        continue

    if not records:
        print("⚠️ Empty DBF:", dbf_path)
        continue

    df = pd.DataFrame(records)
    df["SOURCE_FILE"] = os.path.basename(dbc_path)
    all_dfs.append(df)


# --- MERGE ALL MONTHS ---
if not all_dfs:
    raise SystemExit("❌ No valid DBF files read.")

df = pd.concat(all_dfs, ignore_index=True)
print("✅ Loaded total records:", len(df))

# --- DETECT RELEVANT COLUMNS ---
possible_cnes = [c for c in df.columns if "CNES" in c.upper() or "ESTAB" in c.upper()]
possible_proc = [c for c in df.columns if "PROC" in c.upper() or "PROCED" in c.upper()]
possible_mun = [c for c in df.columns if "MUNIC" in c.upper() or "COD_MUN" in c.upper()]

if not (possible_cnes and possible_proc and possible_mun):
    print("Columns:", df.columns.tolist())
    raise SystemExit("⚠️ Could not detect CNES/PROC/MUNIC columns.")

cnes_col = possible_cnes[0]
proc_col = possible_proc[0]
mun_col = possible_mun[0]
print(f"Using columns → CNES: {cnes_col}, PROC: {proc_col}, MUN: {mun_col}")

# --- FILTER ONLY TARGET CNES ---
target_cnes = ["6697054", "2118513"]
df[cnes_col] = df[cnes_col].astype(str).str.strip()
df = df[df[cnes_col].isin(target_cnes)]

# --- FILTER ALSO BY SPECIFIC PROCEDURE CODES ---
target_procs = ["0407030026", "0407030034"]
df[proc_col] = df[proc_col].astype(str).str.strip()
df = df[df[proc_col].isin(target_procs)]

print(f"✅ Filtered to {len(df)} records for target CNES {target_cnes} "
      f"and PROC {target_procs}")

# --- DEBUG LOGS DE MAPEAMENTO ---
print("\n🔹 Verificando algumas linhas antes do mapeamento:")
for i, row in df.head(10).iterrows():
    print(f"LINHA {i}: CNES='{row[cnes_col]}', PROC='{row[proc_col]}', MUN='{row[mun_col]}'")


# --- NORMALIZE AND MAP ---
df[cnes_col] = df[cnes_col].astype(str).str.strip()
df[proc_col] = df[proc_col].astype(str).str.strip()
df[mun_col] = df[mun_col].astype(str).str.strip()

# --- DEBUG: verificar mapas ---
print(f"\n🔹 Exemplos de PROC_MAP: {list(proc_map.items())[:5]}")
print(f"\n🔹 Exemplos de ESTAB_MAP: {list(estab_map.items())[:5]}")
print(f"\n🔹 Exemplos de MUN_MAP: {list(mun_map.items())[:5]}")

# --- LOG: testar mapeamento linha a linha ---
print("\n🔹 Teste de mapeamento das primeiras 10 linhas:")
for i, row in df.head(10).iterrows():
    hospital_name = estab_map.get(row[cnes_col], f"CNES_{row[cnes_col]}")
    proc_name = proc_map.get(row[proc_col], f"PROC_{row[proc_col]}")
    municipio_name = mun_map.get(row[mun_col], f"MUN_{row[mun_col]}")
    print(f"LINHA {i} -> CNES: {row[cnes_col]} -> {hospital_name}, "
          f"PROC: {row[proc_col]} -> {proc_name}, "
          f"MUN: {row[mun_col]} -> {municipio_name}")

df["HOSPITAL_NAME"] = df[cnes_col].map(estab_map).fillna("CNES_" + df[cnes_col])
df["PROC_NAME"] = df[proc_col].map(proc_map).fillna("PROC_" + df[proc_col])
df["MUNICIPIO_NAME"] = df[mun_col].map(mun_map).fillna("MUN_" + df[mun_col])

# --- KEEP SPECIFIC COLUMNS ---
wanted_cols = [
    "VAL_SH", "VAL_SH_FED", "VAL_SP", "VAL_SP_FED",
    "VAL_TOT", "VAL_UTI",
    "N_AIH", "QT_DIARIAS", "DIAR_ACOM", "DT_INTER", "DT_SAIDA",
    "IDADE", "MORTE", "CID_MORTE", "GESTOR_CPF", "CNPJ_MANT",
    "PROC_SOLIC", "PROC_REA"
]

# check which exist in dataframe
existing_cols = [c for c in wanted_cols if c in df.columns]
missing_cols = [c for c in wanted_cols if c not in df.columns]

if missing_cols:
    print(f"⚠️ Missing columns not found in DBF: {missing_cols}")
if not existing_cols:
    raise SystemExit("❌ None of the requested columns were found in the DBF data.")

print(f"✅ Keeping columns: {existing_cols}")


# --- FINAL EXPORTS (exact columns, no guessing, no renaming) ---

required_cols = [
    "DIAG_PRINC", "N_AIH", "VAL_SH", "VAL_SH_FED", "VAL_SP", "VAL_SP_FED",
    "VAL_UTI", "VAL_TOT", "FINANC", "FAEC_TP", "QT_DIARIAS", "DIAR_ACOM",
    "DT_INTER", "DT_SAIDA", "IDADE", "MORTE", "CID_MORTE",
    "GESTOR_CPF", "CNPJ_MANT", "PROC_SOLIC", "PROC_REA", "PROC_SEC", "SOURCE_FILE"
]

# verify presence in df
existing_cols = [c for c in required_cols if c in df.columns]
missing_cols = [c for c in required_cols if c not in df.columns]

if missing_cols:
    print(f"⚠️ Missing columns not found in DBF: {missing_cols}")

# keep only what exists
final_cols = ["MUNICIPIO_NAME", "HOSPITAL_NAME", "PROC_NAME"] + existing_cols

# create output DataFrame
out = df[final_cols].copy()

print("\n🔹 Columns being exported:")
print(final_cols)

# save to CSV
csv_all = os.path.join(OUT_DIR, "sih_full_mapped.csv")
out.to_csv(csv_all, index=False, encoding="utf-8-sig")

print(f"\n💾 Saved full CSV → {csv_all}")
print(f"✅ Exported {len(out)} records with {len(final_cols)} columns.")
print("✅ No renaming or aggregation — columns preserved as in DBF.\n")

# preview first rows
print(out.head(5).to_string())
