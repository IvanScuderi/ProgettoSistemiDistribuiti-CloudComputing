import pandas as pd
from livy import LivySession
import textwrap

LIVY_URL = "http://localhost:8998"
DATAFRAME = r"C:\Users\1997i\livy-spark-cluster\data\data_flickr.csv"
N_ELEMENTS = 100

df = pd.read_csv(DATAFRAME)

with LivySession.create(LIVY_URL) as session:
    session.upload("data", df)
    session.run(textwrap.dedent("""
    lat_lon = data.select('latitude', 'longitude')
    """))
    ris = session.download("lat_lon")
    print(ris)
    print(df)
