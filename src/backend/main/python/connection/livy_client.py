import pandas as pd
import pickle
import textwrap
import time
import matplotlib.pyplot as plt
import seaborn as sns
import os
import webbrowser
import gmplot
from livy.client import LivyClient
from livy import LivySession
from livy.models import SessionKind, StatementState, SessionState

# chiave fornitami per l'utilizzo delle Google Maps API
API_KEY = "AIzaSyBkb-aG8mMOhg_XneRvWyzLoPFprA3YcAI"
URL = "http://localhost:8998"
DATA = r"C:\Users\1997i\livy-spark-cluster\data\data_flickr.csv"
PICKLE_PATH = r"C:\Users\1997i\livy-spark-cluster\data\data_flickr.sav"
# path in cui salvare i file html risultanti dall'uso di gmplot per la visualizzazione tramite google map
IMG_PATH = r"C:\Users\1997i\livy-spark-cluster\frontend\static\img"
G_MAP_PATH = r"C:\Users\1997i\livy-spark-cluster\gmap"
HTML_NAME = "map.html"

conf = {
    'spark.memory.offHeap.enabled': 'true',
    'spark.memory.offHeap.size': '3g',
    'spark.driver.memory': '4g',
    'spark.executor.memory': '2g',
}

# il dataset caricato da file csv da problemi con la query dei tag perchè il tipo delle colonne risulta 'object'
# e non permette il cast in fase di esecuzione sul cluster perchè non riconosciuto, per questo motivo ho effettuato
# una fase di preprocessing del dataset andando ad effettuare il casting corretto di ogni colonna ed eliminando
# eventuali valori nulli o duplicati!
# FIX PER FAR FUNZIONARE LO STATEMENT_TAG ANCHE CON IL DATASET CARICATO DA CSV: SOSTITUIRE LA COLONNA 'TAG'
# CON LA MEDESIMA COLONNA PRESA DAL DATASET CARICATO TRAMITE FILE PICKLE (???)
df = pd.read_csv(DATA)
df_fix = pickle.load(open(PICKLE_PATH, 'rb'))

df['tags'] = df_fix['tags']


def create_client():
    # crea un client per accedere all'interfaccia fornitami dal server livy
    return LivyClient(url=URL)


def delete_client(client):
    # elimina il client
    client.close()


def create_session(client, upload=True):
    # crea una sessione per il dato client e automaticamente effettua l'upload del dataset sul cluster
    s = client.create_session(kind=SessionKind.PYSPARK, spark_conf=conf)
    s_id = s.session_id
    if upload:
        state = client.get_session(s_id).state
        while state != SessionState.IDLE:
            time.sleep(1)
            state = client.get_session(s_id).state
        session = LivySession(URL, s_id)
        session.upload("data", df)
    return s_id


def delete_session(client, session_id):
    # elimina la sessione
    client.delete_session(session_id)


def submit_statement(client, session_id, statement, download_name):
    # sottomette uno statement python alla sessione e restituisce il risultato
    # il risultato è un df python con nome sul cluster: download_name
    statement = client.create_statement(session_id, statement)
    stat_id = statement.statement_id
    time.sleep(3)
    while True:
        statement = client.get_statement(session_id, stat_id)
        if statement.state == StatementState.AVAILABLE:
            break
        else:
            time.sleep(3)
    session = LivySession(URL, session_id)
    if type(download_name) == list:
        # mi permette di ottenere entrambi i valori anche per statement con
        # return doppi quale statements_kmeans
        r1 = session.download(download_name[0])
        r2 = session.download(download_name[1])
        return r1, r2
    return session.download(download_name)


# sfrutta le API Google Maps per la visualizzazione su mappa
def statement_lat_lon():
    # il nome del dataset caricato sul cluster sarà sempre 'data'
    ris_name = "lat_lon"
    statement = textwrap.dedent("""
    lat_lon = data.select('latitude', 'longitude')
    """)
    return statement, ris_name


# sfrutta le API Google Maps per la visualizzazione su mappa
def statement_kmeans(k=5):
    # applica l'algoritmo di clustering KMeans al dataset sulle colonne 'latitude' e 'longitude'
    # ottenendo come risultato finale il dataset con l'aggiunta della colonna 'predictions' in cui appunto
    # si trova l'indice del cluster a cui fa riferimento ogni elemento, inoltre un secondo risultato è il dataframe
    # centers che contiene k+1 elementi: k elementi che rappresentano le coordinate dei centri dei cluster calcolati,
    # l'ultimo elemento che mi rappresenta il risultato dell'oggetto evaluator applicato sul cluter (sostanzialmente
    # il valore calcolato del coefficiente di silhouette.
    # la necessità di creare un dataframe 'misto' del genere sta nel fallo che il metodo download dell'oggetto
    # livy.session mi permette di scaricare solo oggetti di tipo spark.DataFrama
    ris_name = ["transformed", "centers"]
    statement = textwrap.dedent("""
    from pyspark.sql.types import StructType, StructField, FloatType
    from pyspark.ml.clustering import KMeans
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.evaluation import ClusteringEvaluator
    
    d = data.drop_duplicates(['latitude', 'longitude'])
    vecAssembler = VectorAssembler(inputCols=["latitude", "longitude"], outputCol="features")
    evaluator = ClusteringEvaluator()
    new_sdf = vecAssembler.transform(d)
    kmeans = KMeans().setK(""" + str(k) + """).setSeed(42).setInitMode('k-means||')
    model = kmeans.fit(new_sdf.select('features'))
    transformed = model.transform(new_sdf)
    evaluation = evaluator.evaluate(transformed)
    transformed = transformed.select("latitude","longitude","views","prediction")
    c = model.clusterCenters()
    
    lat_lon = [(float(e[0]), float(e[1]), float(-1)) for e in c]
    lat_lon.append((float(-1), float(-1), float(evaluation)))
    schema = StructType([StructField('latitude', FloatType(), True), StructField('longitude', FloatType(), True),
        StructField('evaluation', FloatType(), True)])
    centers = spark.createDataFrame(data=lat_lon, schema=schema)
    """)
    return statement, ris_name


# sfrutta le API Google Maps per la visualizzazione su mappa
def statement_directions(n_utenti=3, n_post=10):
    # restituisce in ordine cronologico gli n_post degli n_utenti più popolari in termini di views
    ris_name = 'directions'
    statement = textwrap.dedent("""
    from pyspark.sql.functions import *
    
    owner = data.groupby('owner').agg(sum('views').alias('views'))
    owner = owner.orderBy(desc('views'))
    top = owner.head(""" + str(n_utenti) + """)
    directions = spark.createDataFrame(sc.emptyRDD(),data.schema)
    for elem in top:
        row = elem.asDict()
        ppowner = data.filter(data['owner']==row['owner'])
        ppowner = ppowner.drop_duplicates(['latitude', 'longitude'])
        ppowner = ppowner.orderBy(desc('dateTaken'))
        post = ppowner.head(""" + str(n_post) + """)
        rdd = sc.parallelize(post)
        rdd = spark.createDataFrame(rdd,data.schema)
        directions = directions.union(rdd)
    directions = directions.select('owner','views','latitude','longitude','dateTaken')
    """)
    return statement, ris_name


def statement_tags(num_esclusi, n=20, order_by_mean=False, escludi=True):
    # QUERY CHE CALCOLA IL NUMERO DI VOLTE CHE COMPARE UN TAG IN UN POST
    # E LA MEDIA DI VISUALIZZAZIONI CHE OTTENGONO LE FOTO IN CUI COMPAIONO I VARI TAG
    # RESTITUISCE INOLTRE GLI N MIGLIORI TAG IN BASE ALL'ORDINAMENTO SCELTO:
    # L'ORDINAMENTO DI DEFAULT E' SUL NUMERO DI VOLTA CHE UN TAG VIENE USATO, MENTRE
    # SI PUO' ANCHE SCEGLIERE DI OTTENERE I MIGLORI TAG PER MEDIA VISUALIZZAZIONI
    ris_name = "tags"
    statement = textwrap.dedent("""
    from pyspark.sql.functions import *
    
    d = data.groupby('tags').agg(count('tags').alias('count'), mean('views').alias('mean_views'))
    tag = d.select(explode('tags').alias('tag'), 'count', 'mean_views')
    tag = tag.groupBy('tag').agg(sum('count').alias('count'), mean('mean_views').alias('mean_views'))
    ordered_data = tag
    if """ + str(escludi) + """:
        ordered_data = tag.where(tag['count'] > """ + str(num_esclusi) + """)
    if not """ + str(order_by_mean) + """:
        ordered_data = ordered_data.orderBy(desc('count'))
    else:
        ordered_data = ordered_data.orderBy(desc('mean_views'))
    s = ordered_data.schema
    tags = ordered_data.head(""" + str(n) + """)
    tags = sc.parallelize(tags)
    tags = spark.createDataFrame(tags,s)
    """)
    return statement, ris_name


def statement_owner(n=20):
    # restituisce varie informazioni sugli 'n' utenti più attivi,
    # tali utenti sono quelli che postato qualcosa recentemente rispetto agli altri
    ris_name = "utenti"
    statement = textwrap.dedent("""
    from pyspark.sql.functions import *
    from pyspark.sql.window import Window

    user = data.groupby('owner').agg(count('owner').alias('num_post'), mean('views').alias('mean_views'),
        max('datePosted').alias('lastPost'))
    user = user.orderBy(desc('lastPost'))
    user = user.select("*").withColumn('id', row_number().over(Window.orderBy(desc('lastPost'))))
    
    s = user.schema
    utenti = user.head(""" + str(n) + """)
    utenti = sc.parallelize(utenti)
    utenti = spark.createDataFrame(utenti,s)
    
    user = utenti.join(data, utenti.owner == data.owner, "inner")
    tag_per_user = user.groupBy(utenti.owner, 'tags').agg(count('tags').alias('utilizzi'))
    w = Window.partitionBy('owner').orderBy(col('utilizzi').desc())
    tgu = tag_per_user.withColumn('row', row_number().over(w)).where(col("row") == 1)
    utenti = utenti.join(tgu, 'owner', 'inner')
    utenti = utenti.orderBy(desc('lastPost'))
    """)
    return statement, ris_name


def save_plot_tags(ris):
    # metodo che prende come valori il pandas.dataframe risultante dallo statement_tags ed un path
    # e salva il grafico ottenuto (in questo caso sono 2) dal rispettivo dataframe nel suddetto path su disco
    # 1) NUMERO DI OCCORRENZE
    plt.ioff()
    plt.figure(figsize=(8, 10), tight_layout=True)
    sns.barplot(x='count', y='tag', data=ris)
    plt.xlabel("numero di occorrenze")
    plt.ylabel("tag")
    img_name = "occurrence.jpg"
    img_path = os.path.join(IMG_PATH, img_name)
    plt.savefig(img_path)
    # 2) VISUALIZZAZIONI MEDIE
    plt.figure(figsize=(8, 10), tight_layout=True)
    sns.barplot(x='mean_views', y='tag', data=ris)
    plt.xlabel("visualizzazioni medie")
    plt.ylabel("tag")
    img_name = "mean_views.jpg"
    img_path = os.path.join(IMG_PATH, img_name)
    plt.savefig(img_path)


def save_plot_owner(ris):
    utenti = ris.copy()
    utenti['id'] = utenti['id'].astype(str)
    utenti['id'] = "U" + utenti['id']
    # 1) NUMERO DI POST PER UTENTE
    plt.ioff()
    plt.figure(figsize=(7, 10), tight_layout=True)
    sns.barplot(x='num_post', y='id', data=utenti)
    plt.xlabel("numero di post")
    plt.ylabel("id utente")
    img_name = "owner_overview_1.jpg"
    img_path = os.path.join(IMG_PATH, img_name)
    plt.savefig(img_path)
    # 2) VISUALIZZAZIONI MEDIE PER UTENTE
    plt.ioff()
    plt.figure(figsize=(7, 10), tight_layout=True)
    sns.barplot(x='mean_views', y='id', data=utenti)
    plt.xlabel("visualizzazioni medie")
    plt.ylabel("id utente")
    img_name = "owner_overview_2.jpg"
    img_path = os.path.join(IMG_PATH, img_name)
    plt.savefig(img_path)


def show_lat_lon_maps(ris, n_elem):
    # metodo che prende il risultato ottenuto dallo statement_lat_lon
    # e va a plottare su Google Maps un numero di marker, che rappresentano la
    # posizione di dove è stato prodotto il relativo contenuto, pari ad n_elem
    lat = pd.Series.to_list(ris['latitude'])
    lon = pd.Series.to_list(ris['longitude'])
    g_map = gmplot.GoogleMapPlotter.from_geocode("Rome", apikey=API_KEY)
    g_map.scatter(lat[0:n_elem], lon[0:n_elem], color='r', marker=True, size=12)
    path_to_file = os.path.join(G_MAP_PATH, HTML_NAME)
    g_map.draw(path_to_file)
    webbrowser.open('file://' + path_to_file)


def show_directions_maps(ris):
    # metodo che prende il risultato dello statement_directions e lo va a plottare su G Map
    palette = ['red', 'orange', 'yellow', 'green', 'blue', 'purple', 'brown', 'pink', 'white', 'gray', 'lime', 'bisque',
               'gold', 'azure', 'indigo', 'navy', 'aquamarine', 'palegreen', 'wheat', 'olive']
    nome_utenti = ris['owner'].unique()
    num_post_users = ris.groupby(['owner']).count()['views']
    color_dict = {}
    i = 0
    while i < len(nome_utenti):
        # purtroppo non riesco a generare colori diversi in maniera infinita perciò
        # ho selezionato 20 colori diversi e li impiego assegnandoli ai vari utenti in maniera modulare
        # questo comporta che il primo utente ed il 21esimo avranno lo stesso colore per l'itinerario
        color_dict[nome_utenti[i]] = palette[i % len(palette)]
        i += 1
    g_map = gmplot.GoogleMapPlotter.from_geocode("Rome", apikey=API_KEY)
    for row in ris.iterrows():
        # row è una tupla il cui primo elemento è l'indice ed il secondo una Series pandas
        serie = row[1]
        # qualche utente potrebbe utilizzare caratteri speciali nel suo nickname
        # (es: emoticon) che elimino perchè potrebbero portare ad errori di visualizzazione su maps
        # inoltre rimuovo anche gli spazi per una visualizzazione piu compatta
        username = ''.join(e for e in serie['owner'] if e.isalnum())
        latitudine = serie['latitude']
        longitudine = serie['longitude']
        info = "Post by " + username + " " + str(serie['dateTaken']) + " views: " + str(serie['views'])
        color = color_dict[serie['owner']]
        # mi serve per segnare in ordine cronologico su mappa i post di ogni utente
        label = num_post_users.loc[serie['owner']]
        g_map.marker(lat=latitudine, lng=longitudine, color=color, info_window=info, label=label)
        # diminuisco di uno per il prossimo step di plot del marker
        num_post_users.loc[serie['owner']] = num_post_users.loc[serie['owner']] - 1
    path_to_file = os.path.join(G_MAP_PATH, HTML_NAME)
    g_map.draw(path_to_file)
    webbrowser.open('file://' + path_to_file)


def show_kmeans_map(predictions, centers, k, n_post):
    # metodo che va a caricare la pagina html per visualizzare il risultato dello statements_kmeans
    # il risultato sarà una heatmap in cui la colorazione più accesa è data dalla maggior presenza
    # nella zona di post con alte visualizzazioni, verranno inoltri mostrati come marker i centri dei vari
    # cluster ed un numero di marker associati ai post pari a n_post/2 per cluter (per evitare di rendere il risultato
    # troppo confusionari). Si utilizza invece un numero pari ad n_post di elementi per costruire la heatmap
    # anche in questo caso non riuscendo a generare colori all'infinito ho selezionato alcuni colori
    # questi mi serviranno per differenziare gli elementi appartenenti ai vari cluster
    colors = ['red', 'orange', 'yellow', 'green', 'blue', 'purple', 'brown', 'pink', 'white', 'gray',
              'lime', 'bisque', 'gold', 'azure', 'indigo', 'navy', 'aquamarine', 'palegreen', 'wheat', 'olive']
    i = 0
    cluster_list = []
    g_map = gmplot.GoogleMapPlotter.from_geocode("Rome", apikey=API_KEY)
    for elem in centers.iterrows():
        if i == k:
            # ricordo che all'ultima riga del dataframe centers vi è unicamente il valore dell'evaluator
            # per tale motivo la devo skippare (numero di cluster: da 0 a k-1, quando i == k sono sul
            # risultato dell'evaluator)
            break
        cluster = predictions[predictions['prediction'] == i]
        num_elem = len(cluster)
        center = elem[1]
        latitude = center['latitude']
        longitude = center['longitude']
        g_map.marker(lat=latitude, lng=longitude, color='cyan', label="C" + str(i),
                     info_window=("Centroid of cluster " + str(i) + " with " + str(num_elem) + " elements inside"))
        cluster_list.append(cluster)
        i += 1
    i = 0
    for elem in cluster_list:
        ordered_data = elem.sort_values(by=['views'])
        ordered_data = ordered_data.tail(n_post)
        views = ordered_data['views'].to_list()
        latitude = ordered_data['latitude'].to_list()
        longitude = ordered_data['longitude'].to_list()
        color = colors[i % len(colors)]
        g_map.heatmap(lats=latitude, lngs=longitude, radius=30)
        g_map.scatter(lats=latitude[0:int(n_post/2)], lngs=longitude[0:int(n_post/2)], color=color,
                      label=str(i), title=views[0:int(n_post/2)])
        i += 1
    path_to_file = os.path.join(G_MAP_PATH, HTML_NAME)
    g_map.draw(path_to_file)
    webbrowser.open('file://' + path_to_file)
