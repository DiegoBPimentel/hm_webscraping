import pandas as pd
from sqlalchemy import create_engine

path = r'C:\Users\diego\OneDrive\Área de Trabalho\Comunidade DS\repos\Python do ds ao dev\hm_webscraping'
database_name = 'database_hm.sqlite'
conn = create_engine('sqlite:///'+ path + database_name, echo=False )

query_conect = """
    SELECt * FROM vitrine
"""

df_raw = pd.read_sql(query_conect, con=conn)

print(df_raw)