# run.py — pipeline complet UA2 (mrjob, runner=inline)
import os, subprocess, sys, shutil

ROOT = r"C:\Users\Administrator\UA2_Projet_MapReduce"
DATA = r"C:\Users\Administrator\data"
JOBS = os.path.join(ROOT, "jobs")

F_MAIN = os.path.join(DATA, "ventes_multicanal.csv")
F_INC  = os.path.join(DATA, "ventes_increment_2025-10.csv")
F_CAT  = os.path.join(DATA, "catalogue_produits.csv")

OUT_CLEAN  = os.path.join(DATA, "out_clean.tsv")
OUT_JOINED = os.path.join(DATA, "out_joined.tsv")
OUT_KPIS   = os.path.join(DATA, "out_kpis.csv")

DIR_CLEAN   = os.path.join(ROOT, "clean")
DIR_REJECTS = os.path.join(ROOT, "rejects")
DIR_METRICS = os.path.join(ROOT, "metrics")
DIR_TOP10   = os.path.join(ROOT, "top10")

def ensure_dirs():
    os.makedirs(DATA, exist_ok=True)
    for d in [DIR_CLEAN, DIR_REJECTS, DIR_METRICS, DIR_TOP10]:
        os.makedirs(d, exist_ok=True)

def run_cmd(cmd, log_path=None):
    print(">>", " ".join(cmd))
    with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as p:
        out, err = p.communicate()
        if log_path:
            with open(log_path, "w", encoding="utf-8") as f:
                f.write("STDOUT:\n"+out+"\n\nSTDERR:\n"+err)
        if p.returncode != 0:
            print(err)
            raise SystemExit(p.returncode)
        return out, err

def main():
    ensure_dirs()

    # 1) Clean + dedup
    cmd1 = [
        sys.executable, os.path.join(JOBS, "clean_dedup.py"),
        "-r", "inline",
        F_MAIN, F_INC
    ]
    with open(OUT_CLEAN, "w", encoding="utf-8", newline="") as fout:
        p = subprocess.Popen(cmd1, stdout=fout, stderr=subprocess.PIPE, text=True)
        _, err = p.communicate()
        open(os.path.join(ROOT, "log_clean_dedup.txt"), "w", encoding="utf-8").write(err or "")

    # 2) Join catalog
    cmd2 = [
        sys.executable, os.path.join(JOBS, "join_catalog.py"),
        "-r", "inline",
        "--catalog", F_CAT
    ]
    with open(OUT_JOINED, "w", encoding="utf-8", newline="") as fout:
        p = subprocess.Popen(cmd2, stdin=open(OUT_CLEAN, "r", encoding="utf-8"),
                             stdout=fout, stderr=subprocess.PIPE, text=True)
        _, err = p.communicate()
        open(os.path.join(ROOT, "log_join_catalog.txt"), "w", encoding="utf-8").write(err or "")

    # 3) Sales KPIs
    cmd3 = [
        sys.executable, os.path.join(JOBS, "sales_kpis.py"),
        "-r", "inline"
    ]
    with open(OUT_KPIS, "w", encoding="utf-8", newline="") as fout:
        p = subprocess.Popen(cmd3, stdin=open(OUT_JOINED, "r", encoding="utf-8"),
                             stdout=fout, stderr=subprocess.PIPE, text=True)
        _, err = p.communicate()
        open(os.path.join(ROOT, "log_sales_kpis.txt"), "w", encoding="utf-8").write(err or "")

    # Organisation des livrables
    shutil.copy2(OUT_CLEAN, os.path.join(DIR_CLEAN, "out_clean.tsv"))
    shutil.copy2(OUT_KPIS, os.path.join(DIR_METRICS, "out_kpis.csv"))

    with open(OUT_KPIS, "r", encoding="utf-8") as f, \
         open(os.path.join(DIR_TOP10, "top10_products.csv"), "w", encoding="utf-8") as g:
        for line in f:
            if line.startswith("T10,"):
                g.write(line)

    print("\nALL DONE ✅")
    print("clean :", os.path.join(DIR_CLEAN, "out_clean.tsv"))
    print("joined:", OUT_JOINED)
    print("kpis  :", os.path.join(DIR_METRICS, "out_kpis.csv"))
    print("top10 :", os.path.join(DIR_TOP10, "top10_products.csv"))

if __name__ == "__main__":
    main()
