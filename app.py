import io, os, shutil, subprocess, sys, time
import pandas as pd
import plotly.express as px
import streamlit as st

BASE      = os.environ.get("BDT_BASE", r"D:\BigDataTools")
HDFS_URI  = os.environ.get("BDT_HDFS_URI", "hdfs://localhost:9000")
HDFS_CLEAN = "/data/cuaca/clean"
HDFS_HASIL = "/data/cuaca/hasil"
DIR_INPUT = os.path.join(BASE, "data", "input")
DIR_CLEAN = os.path.join(BASE, "data", "clean")
DIR_SCRIPTS = os.path.join(BASE, "scripts")
FILE_UPLOAD = os.path.join(DIR_INPUT, "cuaca_upload.csv")
FILE_CLEAN  = os.path.join(DIR_CLEAN, "cuaca_bersih.csv")
HDFS_BIN = shutil.which("hdfs") or "hdfs"
TABEL = ["bi_rata_bulanan", "bi_pola_jam", "bi_per_stasiun", "bi_korelasi_hari_lampau",
         "rekomendasi_prediksi_14hari", "rekomendasi_ringkasan_negara"]
WAJIB = ["STATION","Station_name","DATE","LATITUDE","LONGITUDE","temperature","dew_point_temperature",
         "sea_level_pressure","wind_speed","precipitation","relative_humidity","visibility","COUNTRY_CODE","temperature_Quality_Code"]
WARNA = {"SIAGA PANAS":"#d62728","WASPADA":"#f2b01e","NORMAL":"#2ca02c"}

st.set_page_config(page_title="Big Data Cuaca Asia Tenggara", layout="wide")

def hdfs(*args, timeout=600):
    try:
        return subprocess.run([HDFS_BIN, *args], capture_output=True, timeout=timeout)
    except (FileNotFoundError, subprocess.TimeoutExpired) as e:
        return subprocess.CompletedProcess([HDFS_BIN, *args], 127, b"", f"Perintah hdfs gagal: {e}. Pastikan HDFS menyala dan hdfs ada di PATH.".encode())

def jalankan_live(cmd, env=None, cwd=None):
    """Jalankan perintah dan tampilkan lognya langsung di halaman."""
    kotak, baris = st.empty(), []
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                         encoding="utf-8", errors="replace", env=env, cwd=cwd)
    for line in p.stdout:
        line = line.rstrip()
        if line and "WARN" not in line and "Stage" not in line:
            baris.append(line); kotak.code("\n".join(baris[-18:]))
    p.wait()
    return p.returncode

@st.cache_data(show_spinner=False)
def baca_tabel(nama, versi):
    r = hdfs("dfs", "-cat", f"{HDFS_HASIL}/{nama}/part-*.csv")
    if r.returncode != 0 or not r.stdout.strip():
        return None
    return pd.read_csv(io.BytesIO(r.stdout))

def hasil_tersedia():
    return hdfs("dfs", "-test", "-e", f"{HDFS_HASIL}/rekomendasi_ringkasan_negara/_SUCCESS").returncode == 0

def mtime_hdfs(path):
    """Waktu modifikasi file di HDFS (detik sejak epoch), None bila file tidak ada."""
    r = hdfs("dfs", "-stat", "%Y", path) 
    try:
        return int(r.stdout.decode().strip()) / 1000 if r.returncode == 0 else None
    except ValueError:
        return None

# - SIDEBAR -
st.sidebar.title("Big Data Cuaca ASEAN")
halaman = st.sidebar.radio("Menu", ["1. Input & Proses Data", "2. Hasil Analisis & Rekomendasi"])
if st.sidebar.button("Muat ulang data dari HDFS"):
    st.session_state["versi"] = time.time()
versi = st.session_state.get("versi", 0)

# - HALAMAN 1 -
if halaman.startswith("1"):
    st.title("Input & Proses Data")
    st.caption("Alur: upload CSV -> Modul 1 (pre-processing + cleaning + normalisasi) -> NiFi -> HDFS -> analisis Spark. Hasilnya ada di menu 2.")

    st.subheader("Langkah 1. Upload file CSV")
    up = st.file_uploader("File CSV cuaca (nama bebas, kolom harus sesuai format data NOAA ISD)", type="csv")
    if up is not None and st.button("Simpan file upload"):
        os.makedirs(DIR_INPUT, exist_ok=True)
        with open(FILE_UPLOAD, "wb") as f:
            shutil.copyfileobj(up, f, 8 * 1024 * 1024)
        kolom = pd.read_csv(FILE_UPLOAD, nrows=1).columns
        kurang = [c for c in WAJIB if c not in kolom]
        if kurang:
            st.error(f"Kolom wajib tidak ada: {kurang}")
        else:
            st.success(f"Tersimpan: {FILE_UPLOAD} ({os.path.getsize(FILE_UPLOAD)/1e6:,.0f} MB). Kolom wajib lengkap.")

    st.subheader("Langkah 2. Pre-processing, cleaning, normalisasi")
    sumber = st.text_input("File sumber", "", placeholder=r"D:\BigDataTools\data\input\namafile.csv").strip().strip('"')
    if st.button("Jalankan Modul 1"):
        if not sumber:
            st.error("Isi dulu path file sumber.")
        elif not os.path.exists(sumber):
            st.error(f"File tidak ditemukan: {sumber}")
        else:
            os.makedirs(DIR_CLEAN, exist_ok=True)
            st.session_state["modul1_mulai"] = time.time()   # patokan: file di HDFS harus lebih baru dari ini
            kode = jalankan_live([sys.executable, os.path.join(DIR_SCRIPTS, "modul1_preprocess.py"), sumber, FILE_CLEAN])
            if kode == 0:
                st.success("Modul 1 selesai.")
            else:
                st.error("Modul 1 gagal, lihat log di atas.")

    st.subheader("Langkah 3. Kirim ke HDFS")
    mode = st.radio("Cara kirim", ["Lewat NiFi (flow GetFile -> PutHDFS harus sudah Start)", "Langsung hdfs put (cadangan, Stop GetFile dulu)"])
    if st.button("Kirim ke HDFS"):
        if mode.startswith("Lewat"):
            st.info("Menunggu NiFi memindahkan file ke HDFS (maks 5 menit)...")
            tujuan = f"{HDFS_CLEAN}/cuaca_bersih.csv"
            t_mulai = st.session_state.get("modul1_mulai")
            ok = False
            for _ in range(60):
                m = mtime_hdfs(tujuan)
                if m is not None:
                    if (m >= t_mulai) if t_mulai else (not os.path.exists(FILE_CLEAN)):
                        ok = True; break
                time.sleep(5)
            if ok:
                st.success("File baru sudah ada di HDFS (menimpa file lama bila ada).")
            else:
                st.warning("Belum muncul. Cek flow NiFi (GetFile & PutHDFS sudah Start?), pastikan Modul 1 sudah dijalankan, atau pakai mode cadangan.")
        else:
            hdfs("dfs", "-mkdir", "-p", HDFS_CLEAN)
            r = hdfs("dfs", "-put", "-f", FILE_CLEAN, HDFS_CLEAN + "/")
            if r.returncode == 0:
                st.success("Terkirim ke HDFS.")
            else:
                st.error(r.stderr.decode(errors="replace"))
    r = hdfs("dfs", "-ls", "-h", HDFS_CLEAN)
    if r.returncode == 0:
        st.code(r.stdout.decode(errors="replace"))

    st.subheader("Langkah 4. Jalankan analisis Spark (BI + prediksi 14 hari, data dibaca dari HDFS)")
    mem = st.selectbox("Memori Spark", ["4g", "6g", "8g"])
    if st.button("Jalankan Analisis Spark"):
        env = {k: v for k, v in os.environ.items() if k.upper() not in ("SPARK_HOME", "PYTHONPATH")}
        env.update(PYSPARK_SUBMIT_ARGS=f"--driver-memory {mem} pyspark-shell", PYTHONIOENCODING="utf-8",
                   PYSPARK_PYTHON=sys.executable, PYSPARK_DRIVER_PYTHON=sys.executable)
        kode = jalankan_live([sys.executable, os.path.join(DIR_SCRIPTS, "modul2_spark.py"),
                              HDFS_URI + HDFS_CLEAN + "/cuaca_bersih.csv", HDFS_URI + HDFS_HASIL], env=env)
        if kode == 0:
            st.session_state["versi"] = time.time()
            st.success("Analisis selesai. Lihat hasilnya di menu 2. Hasil Analisis & Rekomendasi.")
        else:
            st.error("Spark gagal, lihat log di atas.")


# - HALAMAN 2 -
else:
    st.title("Modul 2: Hasil Analisis & Rekomendasi Cuaca Asia Tenggara")
    versi = st.session_state.get("versi", 0)
    if not hasil_tersedia():
        st.warning("Hasil analisis belum ada di HDFS. Jalankan dulu menu 1: Modul 1, kirim ke HDFS, lalu Jalankan Analisis Spark (Langkah 4).")
        st.stop()
    T = {n: baca_tabel(n, versi) for n in TABEL}
    if any(v is None for v in T.values()):
        st.error("Sebagian tabel hasil tidak terbaca dari HDFS: " + ", ".join(n for n, v in T.items() if v is None))
        st.stop()

    bulanan, jam, stasiun, kor = T["bi_rata_bulanan"], T["bi_pola_jam"], T["bi_per_stasiun"], T["bi_korelasi_hari_lampau"]
    besok, ring = T["rekomendasi_prediksi_14hari"], T["rekomendasi_ringkasan_negara"]

    daftar = sorted(bulanan["negara"].unique())
    pilih = st.sidebar.multiselect("Filter negara", daftar, default=daftar)
    f = lambda d: d[d["negara"].isin(pilih)] if "negara" in d.columns else d
    bulanan, jam, stasiun, besok, ring = map(f, [bulanan, jam, stasiun, besok, ring])

    tab1, tab2 = st.tabs(["Insight (BI)", "Rekomendasi ke depan"])

    with tab1:
        c = st.columns(4)
        c[0].metric("Rata-rata suhu", f"{(bulanan['rata_suhu']*bulanan['jumlah_observasi']).sum()/bulanan['jumlah_observasi'].sum():.1f} °C")
        c[1].metric("Jumlah observasi", f"{int(bulanan['jumlah_observasi'].sum()):,}")
        c[2].metric("Jumlah stasiun", f"{len(stasiun):,}")
        c[3].metric("Negara", len(pilih))
        st.plotly_chart(px.line(bulanan, x="tahun_bulan", y="rata_suhu", color="negara", markers=True,
                                title="Rata-rata suhu per bulan", labels={"rata_suhu": "Suhu (°C)", "tahun_bulan": "Bulan"}), width="stretch")
        a, b = st.columns(2)
        kel = bulanan.groupby("negara").apply(lambda g: (g["rata_kelembapan"]*g["jumlah_observasi"]).sum()/g["jumlah_observasi"].sum(), include_groups=False).reset_index(name="kelembapan")
        a.plotly_chart(px.bar(kel, x="negara", y="kelembapan", title="Rata-rata kelembapan (%)", color="negara"), width="stretch")
        b.plotly_chart(px.line(jam, x="jam", y="rata_suhu", color="negara", title="Pola suhu per jam (UTC, WIB = UTC+7)"), width="stretch")
        st.plotly_chart(px.scatter_map(stasiun, lat="LATITUDE", lon="LONGITUDE", color="rata_suhu", size="jumlah_observasi",
                        hover_name="Station_name", color_continuous_scale="YlOrRd", zoom=3.3, height=520,
                        title="Rata-rata suhu per stasiun"), width="stretch")
        st.plotly_chart(px.bar(kor, x="korelasi_dengan_suhu_hari_ini", y="faktor_hari_lampau", orientation="h",
                        title="Pengaruh kondisi hari lampau terhadap suhu hari ini (korelasi)",
                        labels={"korelasi_dengan_suhu_hari_ini": "Korelasi", "faktor_hari_lampau": ""}), width="stretch")

    with tab2:
        tgl = sorted(besok["tanggal"].unique())
        st.subheader(f"Perkiraan suhu 14 hari ke depan ({tgl[0]} sampai {tgl[-1]})")
        akhir = pd.to_datetime(tgl[0]) - pd.Timedelta(days=1)
        lewat = (pd.Timestamp.today().normalize() - akhir).days
        st.info(f"Prediksi dihitung dari data observasi terakhir tanggal **{akhir.date()}**. "
                f"Perkiraan 'besok' berarti sehari setelah tanggal itu, bukan setelah hari ini. "
                f"Data terakhir {max(lewat, 0)} hari yang lalu; perbarui dataset lalu jalankan ulang Spark untuk memajukan prediksi.")
        tren = ring.groupby(["negara", "tanggal"], as_index=False).agg(rata_prediksi_suhu=("rata_prediksi_suhu", "mean"))
        st.plotly_chart(px.line(tren, x="tanggal", y="rata_prediksi_suhu", color="negara", markers=True,
                        title="Perkiraan rata-rata suhu per negara", labels={"rata_prediksi_suhu": "Suhu (°C)"}), width="stretch")
        pilih_tgl = st.select_slider("Lihat peta untuk tanggal", options=tgl)
        bd = besok[besok["tanggal"] == pilih_tgl]
        c = st.columns(3)
        for col, s_ in zip(c, ["SIAGA PANAS", "WASPADA", "NORMAL"]):
            col.metric(s_, int((bd["status"] == s_).sum()))
        st.plotly_chart(px.scatter_map(bd, lat="LATITUDE", lon="LONGITUDE", color="status", color_discrete_map=WARNA,
                        hover_name="Station_name", hover_data=["suhu_prediksi", "suhu_p90_historis"], zoom=3.3, height=520,
                        title=f"Status stasiun pada {pilih_tgl}"), width="stretch")
        hari_status = besok.groupby(["tanggal", "status"]).size().reset_index(name="stasiun")
        st.plotly_chart(px.bar(hari_status, x="tanggal", y="stasiun", color="status", color_discrete_map=WARNA,
                        title="Jumlah stasiun per status tiap hari"), width="stretch")
        rk = ring[ring["tanggal"] == pilih_tgl]
        st.dataframe(rk, width="stretch", hide_index=True)
        st.dataframe(besok[besok["status"] != "NORMAL"].sort_values(["tanggal", "suhu_prediksi"], ascending=[True, False]),
                     width="stretch", hide_index=True)
        st.info("Status memakai persentil 90 suhu historis tiap stasiun. Makin jauh harinya, makin besar galatnya, jadi prediksi 1 sampai 3 hari lebih dapat diandalkan daripada 10 sampai 14 hari. Vietnam dan Filipina tidak punya data terbaru.")