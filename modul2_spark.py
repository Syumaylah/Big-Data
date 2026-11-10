import os, sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
from pyspark.sql import SparkSession, functions as F, Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor

IN_PATH  = sys.argv[1]
OUT_BASE = sys.argv[2]

spark = SparkSession.builder.appName("Modul2_Analisis_Cuaca").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.shuffle.partitions", "8")

def buat_df(rows, cols):
    """Buat DataFrame kecil dari daftar Python TANPA worker Python (murni mesin Spark/Java)."""
    structs = [F.struct(*[F.lit(v).alias(c) for v, c in zip(r, cols)]) for r in rows]
    return spark.range(1).select(F.explode(F.array(*structs)).alias("r")).select("r.*")

def simpan(df, nama):
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{OUT_BASE}/{nama}")
    print(f"  tersimpan: {OUT_BASE}/{nama}")

# - 1. BACA DARI HDFS -
KOLOM = ["STATION", "Station_name", "DATE", "LATITUDE", "LONGITUDE", "temperature", "dew_point_temperature",
         "sea_level_pressure", "wind_speed", "relative_humidity", "negara", "jam"]
raw = spark.read.option("header", True).option("inferSchema", True).csv(IN_PATH)
df = raw.select(*KOLOM).withColumn("DATE", F.to_timestamp("DATE")).cache()
print("Jumlah baris dibaca dari HDFS:", df.count())

# - 2. BUSINESS INTELLIGENCE -
print("\n[BI] Agregasi...")
bulanan = (df.withColumn("tahun_bulan", F.date_format("DATE", "yyyy-MM"))
  .groupBy("negara", "tahun_bulan")
  .agg(F.round(F.avg("temperature"), 2).alias("rata_suhu"), F.round(F.min("temperature"), 1).alias("suhu_min"),
       F.round(F.max("temperature"), 1).alias("suhu_max"), F.round(F.avg("relative_humidity"), 2).alias("rata_kelembapan"),
       F.round(F.avg("wind_speed"), 2).alias("rata_angin"), F.round(F.avg("sea_level_pressure"), 2).alias("rata_tekanan"),
       F.countDistinct("STATION").alias("jumlah_stasiun"), F.count("*").alias("jumlah_observasi"))
  .orderBy("negara", "tahun_bulan"))
simpan(bulanan, "bi_rata_bulanan")

pola_jam = (df.groupBy("negara", "jam").agg(F.round(F.avg("temperature"), 2).alias("rata_suhu"),
            F.round(F.avg("relative_humidity"), 2).alias("rata_kelembapan")).orderBy("negara", "jam"))
simpan(pola_jam, "bi_pola_jam")

stasiun = (df.groupBy("STATION", "Station_name", "negara", "LATITUDE", "LONGITUDE")
  .agg(F.round(F.avg("temperature"), 2).alias("rata_suhu"), F.round(F.max("temperature"), 1).alias("suhu_max"),
       F.round(F.avg("relative_humidity"), 2).alias("rata_kelembapan"), F.count("*").alias("jumlah_observasi")))
simpan(stasiun, "bi_per_stasiun")

# - 3. DATASET HARIAN + FITUR "TITIK ASAL" (hari t) -
print("\n[ML] Membangun fitur hari lampau...")
harian = (df.withColumn("hari", F.to_date("DATE"))
  .groupBy("STATION", "Station_name", "negara", "hari")
  .agg(F.avg("temperature").alias("suhu"), F.avg("dew_point_temperature").alias("embun"),
       F.avg("relative_humidity").alias("lembap"), F.avg("wind_speed").alias("angin"),
       F.first("LATITUDE").alias("lat"), F.first("LONGITUDE").alias("lon"))).cache()

def geser(kolom, k, nama):   # nilai hari (d-k) ditaruh di baris hari d
    return harian.select("STATION", F.date_add("hari", k).alias("hari"), F.col(kolom).alias(nama))

asal = (harian.withColumn("suhu_t0", F.col("suhu")).withColumn("lembap_t0", F.col("lembap"))
              .withColumn("angin_t0", F.col("angin")).withColumn("embun_t0", F.col("embun")))
for k in (1, 2, 6):
    asal = asal.join(geser("suhu", k, f"suhu_t{k}"), ["STATION", "hari"], "left")
asal = asal.withColumn("hari_n", F.datediff("hari", F.lit("1970-01-01")))
w = Window.partitionBy("STATION").orderBy("hari_n").rangeBetween(-6, 0)
asal = (asal.withColumn("suhu_rata7", F.avg("suhu").over(w)).withColumn("n7", F.count("suhu").over(w))
            .filter(F.col("n7") >= 3).cache())

FITUR_ASAL = ["suhu_t0", "suhu_t1", "suhu_t2", "suhu_t6", "suhu_rata7", "lembap_t0", "angin_t0", "embun_t0"]
FITUR = FITUR_ASAL + ["bulan_target", "lat", "lon"]
HORIZON = list(range(1, 15))
hari_akhir = harian.agg(F.max("hari")).first()[0]
print("Data terakhir:", hari_akhir, "-> prediksi +1 s/d +14 hari")

def target(h):   # suhu hari (d+h)
    return harian.select("STATION", F.date_sub("hari", h).alias("hari"), F.col("suhu").alias("y"))

# - 4. BI: PENGARUH HARI LAMPAU (korelasi) -
bi_data = asal.join(target(1), ["STATION", "hari"]).dropna(subset=FITUR_ASAL)
pasangan = [("suhu_t0", "Suhu 1 hari lalu"), ("suhu_t1", "Suhu 2 hari lalu"), ("suhu_t2", "Suhu 3 hari lalu"),
            ("suhu_t6", "Suhu 7 hari lalu"), ("suhu_rata7", "Rata-rata suhu 7 hari lalu"),
            ("lembap_t0", "Kelembapan 1 hari lalu"), ("angin_t0", "Kecepatan angin 1 hari lalu"),
            ("embun_t0", "Titik embun 1 hari lalu")]
kor_row = bi_data.agg(*[F.corr("y", k).alias(k) for k, _ in pasangan]).first()   # 1 job untuk semua korelasi
baris = [(label, round(float(kor_row[k]), 3)) for k, label in pasangan]
simpan(buat_df(baris, ["faktor_hari_lampau", "korelasi_dengan_suhu_hari_ini"]), "bi_korelasi_hari_lampau")

# - 5. RANDOM FOREST, (+1 ... +14 hari) -
va = VectorAssembler(inputCols=FITUR, outputCol="features")
awal_pred = asal.filter(F.col("hari") == F.lit(hari_akhir)).dropna(subset=FITUR_ASAL).cache()
ambang = harian.groupBy("STATION").agg(F.expr("percentile_approx(suhu, 0.9)").alias("suhu_p90")).cache()

besok_all = None
for h in HORIZON:
    data = (asal.join(target(h), ["STATION", "hari"]).dropna(subset=FITUR_ASAL + ["y"])
                .withColumn("bulan_target", F.month(F.date_add("hari", h))))
    model = RandomForestRegressor(featuresCol="features", labelCol="y", numTrees=50, maxDepth=10, seed=42).fit(va.transform(data))
    print(f"Horizon +{h}: model selesai dilatih")
    dep = awal_pred.withColumn("bulan_target", F.month(F.date_add("hari", h)))
    b = (model.transform(va.transform(dep)).join(ambang, "STATION", "left")
         .withColumn("suhu_prediksi", F.round("prediction", 2))
         .withColumn("status", F.when(F.col("suhu_prediksi") >= F.col("suhu_p90") + 0.5, "SIAGA PANAS")
                                .when(F.col("suhu_prediksi") >= F.col("suhu_p90") - 0.5, "WASPADA").otherwise("NORMAL"))
         .select("STATION", "Station_name", "negara", F.col("lat").alias("LATITUDE"), F.col("lon").alias("LONGITUDE"),
                 F.lit(h).alias("horizon"), F.date_add(F.lit(hari_akhir), h).alias("tanggal"), "suhu_prediksi",
                 F.round("suhu_p90", 2).alias("suhu_p90_historis"), "status"))
    besok_all = b if besok_all is None else besok_all.unionByName(b)

besok_all = besok_all.cache()
simpan(besok_all, "rekomendasi_prediksi_14hari")

ringkas = (besok_all.groupBy("negara", "horizon", "tanggal")
  .agg(F.round(F.avg("suhu_prediksi"), 2).alias("rata_prediksi_suhu"),
       F.sum(F.when(F.col("status") == "SIAGA PANAS", 1).otherwise(0)).alias("stasiun_siaga_panas"),
       F.sum(F.when(F.col("status") == "WASPADA", 1).otherwise(0)).alias("stasiun_waspada"),
       F.count("*").alias("jumlah_stasiun")).orderBy("negara", "horizon"))
simpan(ringkas, "rekomendasi_ringkasan_negara")   # ditulis terakhir: dipakai app sebagai penanda hasil sudah lengkap
spark.stop()