################################################################################################
#######################             IMPORAMOS LIBRERÍAS              ###########################
################################################################################################


import requests
import json 
import pandas as pd

################################################################################################
#######################          EXTRACCIÓN Y CARGA DATOS              #########################
################################################################################################

# 1. LLAMADA - EXTRACCIÓN API USUARIOS



def api_rest(url):
  try:
    response = requests.get(url)  
    response.raise_for_status()  # LLamamos errores de excepción (>= 400)
  
    # Lectura fichero JSON definido en data
    data_api = response.json()
    df=pd.json_normalize(data_api) # Covertismos JSON a Dataframe normalizándolo
    print("Respuesta exitosa")
    return df

  except requests.exceptions.RequestException as e:
    # Imprimimos el tipo de error de solicitud (e.g., connection errors, timeouts)
    print(f"Error: {type(e).__name__} - {e}")

  except json.JSONDecodeError as e:
    # Imprimimos el error de JSON data propio del la lectura y no de la solictud
    print(f"Error: JSON parsing error - {e}")





# 2. LECTURA - EXTRACCIÓN CSV
def csv_read(csv_route, delimiter=';'):
  return pd.read_csv(csv_route)



# df_usuarios=api_rest(url)
# df_reservas=csv_read(csv_route)



################################################################################################
#########################     LIMPIEZA Y TRANSFORMACIÓN DATOS        ###########################
################################################################################################

# 3. Comprobamos que los datos se hallan cargado correctamente en formato df y su resumen

def resumen_df(df):
  print("\n\nDatos usuarios:\n\n", df.head(3))
  print("\n")
  print(df.info())



  print("Total registros reservas sin eliminar duplicados: ", len(df))

# df_usuarios = resumen_df(df_usuarios)
# df_reservas = resumen_df(df_reservas)

"""
COMENTARIOS:
De tabla USUARIOS campo "phone" debería ser numérico pero al tener caracteres lo dejaremos como tal
De tabla RESERVAS no vienen en el formato correcto los siguientes campos:

-"agent" que viene en formato float cuando debe ser int ya que es el campo de unión con el campo "id" de la otra tabla
-"reservation_status_date" viene en formato object cuando es de formato date
- Cuestionable son campos "arrival_date_year", "arrival_date_month" y "arrival_date_day_of_month" que vienen separados y deberían estar unidos bajo formato date

"""

# 4. Tratamos duplicados y datos nulos

## 4.1 Eliminadmos duplicados

def no_duplicados (df):
  df = df.drop_duplicates()
  

  print("\n")
  print("Total registros usuarios eliminando duplicados: ", len(df))


  """
  COMENTARIO:
  En USUARIOS no existen duplicados, pero si en RESERVAS reduciéndose de 56707 a 40485 tras eliminarlos

  """
  return df
  
  
# df_reservas_unicos = no_duplicados (df_reservas)
# df_usuarios_unicos = no_duplicados (df_usuarios) 


## 4.2 Tratamos nulos

### Analizamos en qué columnas de cada tabla existen nulos
def existen_nulos (df):
  df_col=df.columns
  df = [x for x in df_col if df[x].isnull().sum() > 0]
  print("\n")
  print("En usuarios existen faltantes en columnas: ", df)
  return df


# faltantes_reservas = existen_nulos(df_reservas_unicos)
# faltantes_usuarios = existen_nulos(df_usuarios_unicos)

## Comprobamos que existe un solo identificador de agente por country para poder reemplazar dichos nulos con la información de City de USUARIOS
#df_reservas_res = df_reservas_unicos[["agent", "country"]]
#print("\n")
#print("Identificadores de agente por país: ", df_reservas_res)

## Removemos nulos de 'agent' y 'country'

def drop_na (df, col):
  df = df.copy()
  df.dropna(subset=[col], inplace=True)
  print("\n")
  print("Reservas sin dupicados ni nulos en 'agent': ", len(df))
  print("\n")
  return df


# df_reservas_unicos_sin_na = drop_na (df_reservas_unicos, 'agent')




## 4.3 Renombramos columnas reemplazando . por _ de tabla df_usuarios

def rename_replace(df):

  new_colnames = df.columns.str.replace('.', '_')
  df = df.rename(columns=dict(zip(df.columns, new_colnames)))
  return df


"""
COMENTARIO:
De ambas tablas solo existen datos nulos en la tabla de RESERVAS en los campos 'country' y 'agent'

Debido a que del campo 'agent' no existe información con la que se pueda tratar estos nulos, procederemos a quitarlos ya que no 
los podremos unir a la tablas de USUARIOS mediante el campo 'id'. Además, hay que cambiar el formato de 'agent' para que sirva 
realizar la unión con 'id' y sin quitar los nulos no es posible

Por su parte, los nulos de la tabla 'country' no se pueden dedurcir una vez realizemos la unión con los usuarios, al existir en esta 
última tabla información de la dirección de la ciudad de los usuarios pero no de los clientes que hacen las reservas. No obstante,
al contener información en las demás columnas no las eliminaremos, ya que solo afectarán a la dimensionalidad de 'country', pudiendo
excluirse los nulos cuando debamos representar algo a nivel de 'country'

Tras eliminar nulos de 'agent' nos quedamos en RESERVAS con 34853 registros

"""



## 4.4 Tratamos formatos incorrecto de campos de RESERVAS, y de USUARIOS 

def int_format (df, col):
  df[col] = df[col].astype(dtype='int64')
  return df.info()

def date_dmy_format (df, col):
  df[col] = pd.to_datetime(df[col], format='%d/%m/%Y')
  return df.info()

def float_format (df, col):
  df[col] = df[col].astype(dtype='float')
  return df.info()

def monthB_format (df, col):
  df[col] = pd.to_datetime(df[col], format='%B').dt.month
  return df.info()


def date_str (df, new_col, col1, col2, col3):
  df[new_col] = df[[col1, col2, col3]].astype(str).agg(''.join, axis=1)
  return df.info()

def date_ymd_format (df, new_col, col):
  df[new_col] = pd.to_datetime(df[col], format='%Y%m%d' )
  return df.info()

def drop_col (df, col):
  df = df.drop([col], axis=1)
  return df



## 4.5 Guardamos ficheros csv para que Postgres SQL los lea con su fecha de recarga

def date_load_field (df,filename):
  today = pd.Timestamp('today')
  numeric_date = today.timestamp()
  numeric_date = int(numeric_date)
  
  df['date_load'] = pd.Timestamp.today().strftime('%Y-%m-%d')
  df['date_load_numeric'] = numeric_date
  
  return df.to_csv(fr'C:\Users\aronm\Documents\Proyecto ETL\{filename}.csv', index=False)

  




