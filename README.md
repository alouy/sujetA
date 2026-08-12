Sujet A — Pipeline Spark → MongoDB (avis clients e-commerce)

Pipeline de traitement des avis clients Amazon (dataset Reviews.csv, Amazon Fine Food Reviews) avec PySpark pour le traitement et MongoDB pour le stockage des résultats. Environnement conteneurisé avec Docker Compose.

Prérequis
Docker + Docker Compose
Le fichier Reviews.csv (non inclus dans ce dépôt, voir .gitignore — trop volumineux pour GitHub)
Étape 1 — Préparer le dossier de travail
bash
mkdir -p ~/sujetA/data ~/sujetA/notebooks
cp /media/sf_Downloads/Reviews.csv ~/sujetA/data/
cd ~/sujetA
Étape 2 — docker-compose.yml (Spark + MongoDB)
yaml
services:
  mongodb:
    image: mongo:7
    container_name: sujetA-mongo
    ports:
      - "27017:27017"
    volumes:
      - mongo_data:/data/db

  pyspark:
    image: quay.io/jupyter/pyspark-notebook:spark-3.5.3
    container_name: sujetA-pyspark
    ports:
      - "8888:8888"
    volumes:
      - ./data:/home/jovyan/work/data
      - ./notebooks:/home/jovyan/work/notebooks
    depends_on:
      - mongodb

volumes:
  mongo_data:

Note : la clé version: a été retirée (obsolète avec Docker Compose v2).

Lancement :

bash
docker compose up -d
docker ps
Étape 3 — Accéder à Jupyter
bash
docker logs sujetA-pyspark

Récupérer l'URL avec le token :

http://127.0.0.1:8888/lab?token=...

⚠️ Le volume ./data est monté sur /home/jovyan/work/data (et non /home/jovyan/data) — bien utiliser les chemins correspondants dans le notebook.

Le notebook de travail est créé dans notebooks/analyse_reviews.ipynb (attention à bien l'enregistrer/déplacer dans work/notebooks/ depuis JupyterLab, sinon il reste hors du volume synchronisé).

Étape 4 — Créer la session Spark (avec connecteur MongoDB)
python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SujetA-AvisClients") \
    .config("spark.mongodb.write.connection.uri", "mongodb://sujetA-mongo:27017/sujetA") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0") \
    .getOrCreate()
Étape 5 — Charger et explorer les données
python
df = spark.read.csv(
    "/home/jovyan/work/data/Reviews.csv",
    header=True,
    inferSchema=True,
    quote='"',
    escape='"',
    multiLine=True
)

df.printSchema()
df.show(5)
print("Nombre de lignes :", df.count())

Le dataset contient des guillemets imbriqués dans les champs texte (Summary, Text), d'où l'ajout de quote, escape et multiLine=True pour un parsing correct et un typage fiable des colonnes.

Étape 6 — Nettoyage des données
python
from pyspark.sql.functions import col, from_unixtime, to_date

df_clean = df.dropna(subset=["Score", "Text", "ProductId"]) \
    .withColumn("ReviewDate", to_date(from_unixtime(col("Time").cast("long")))) \
    .drop("Time") \
    .dropDuplicates(["Id"])

df_clean.show(5)
print("Nombre de lignes après nettoyage :", df_clean.count())

Time est casté explicitement en long avant conversion, Score est vérifié pour ne garder que des valeurs numériques propres.

Étape 7 — Traitements / analyses
python
from pyspark.sql.functions import count, avg, round as spark_round, when

# Note moyenne et nombre d'avis par produit
avis_par_produit = df_clean.groupBy("ProductId") \
    .agg(
        count("*").alias("nb_avis"),
        spark_round(avg("Score"), 2).alias("note_moyenne")
    ) \
    .orderBy(col("nb_avis").desc())

avis_par_produit.show(10)

# Répartition des notes (1 à 5)
repartition_notes = df_clean.groupBy("Score").count().orderBy("Score")
repartition_notes.show()

# Taux d'utilité des avis
df_utilite = df_clean.withColumn(
    "taux_utilite",
    when(col("HelpfulnessDenominator") > 0,
         spark_round(col("HelpfulnessNumerator") / col("HelpfulnessDenominator"), 2))
    .otherwise(None)
)
df_utilite.select("Id", "ProductId", "HelpfulnessNumerator", "HelpfulnessDenominator", "taux_utilite").show(5)
Étape 8 — Écrire les résultats dans MongoDB
python
df_clean.write \
    .format("mongodb") \
    .mode("overwrite") \
    .option("database", "sujetA") \
    .option("collection", "avis_nettoyes") \
    .save()

avis_par_produit.write \
    .format("mongodb") \
    .mode("overwrite") \
    .option("database", "sujetA") \
    .option("collection", "avis_par_produit") \
    .save()

repartition_notes.write \
    .format("mongodb") \
    .mode("overwrite") \
    .option("database", "sujetA") \
    .option("collection", "repartition_notes") \
    .save()
Étape 9 — Vérifier dans MongoDB
bash
docker exec -it sujetA-mongo mongosh
javascript
use sujetA
db.avis_nettoyes.countDocuments()
db.avis_par_produit.find().limit(5).pretty()
db.repartition_notes.find()
Étape 10 — Arrêter proprement
bash
docker compose stop

docker compose down supprime les conteneurs (garde les volumes/données) ; docker compose down -v supprime aussi les données Mongo.

Structure du dépôt
sujetA/
├── docker-compose.yml
├── .gitignore
├── README.md
└── notebooks/
    └── analyse_reviews.ipynb

Le dossier data/ (contenant Reviews.csv, ~300 Mo) est exclu du dépôt via .gitignore — trop volumineux pour GitHub.
