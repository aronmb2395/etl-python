from ETL_python import api_rest, csv_read, resumen_df, no_duplicados, existen_nulos, drop_na, rename_replace, int_format, date_dmy_format, float_format, monthB_format, date_str, date_ymd_format, drop_col, date_load_field
import requests
import json 
import pandas as pd
import psycopg2
from psycopg2 import sql

def main():


    url= "https://jsonplaceholder.typicode.com/users" 
    csv_route = r'C:\Users\USUARIO\Documents\Proyecto ETL\reservasHotel.csv'

    def etl():
        
        # EXTRACT
        
        df_usuarios=api_rest(url)  
        df_reservas=csv_read(csv_route)
        
        

        # TRANSFORM

        ## Eliminadmos duplicados

        df_usuarios_unicos = no_duplicados(df_usuarios)
        df_reservas_unicos = no_duplicados(df_reservas)

        
        ### Analizamos en qué columnas de cada tabla existen nulos
        faltantes_reservas = existen_nulos(df_reservas_unicos)
        faltantes_usuarios = existen_nulos(df_usuarios_unicos)

        ## Removemos nulos de 'agent'
        df_reservas_unicos_sin_na = drop_na (df_reservas_unicos, 'agent')

        ## Renombramos columnas reemplazando . por _ de tabla df_usuarios

        df_usuarios_unicos =rename_replace(df_usuarios_unicos)
        print(df_reservas_unicos.columns)

            
        ## Tratamos formatos incorrecto de campos
        print(int_format (df_reservas_unicos_sin_na, 'agent'))

        print(date_dmy_format (df_reservas_unicos_sin_na, 'reservation_status_date'))

        print(float_format (df_usuarios_unicos, 'address_geo_lat'))
        print(float_format (df_usuarios_unicos, 'address_geo_lng')) 

        print(monthB_format (df_reservas_unicos_sin_na, 'arrival_date_month'))
        print(date_str(df_reservas_unicos_sin_na, 'arrival_date_str', 'arrival_date_year', 'arrival_date_month', 'arrival_date_day_of_month'))

        print(date_ymd_format(df_reservas_unicos_sin_na, 'arrival_date', 'arrival_date_str'))

        df_reservas_unicos_sin_na= drop_col (df_reservas_unicos_sin_na, 'arrival_date_year')
        df_reservas_unicos_sin_na =drop_col (df_reservas_unicos_sin_na, 'arrival_date_month')
        df_reservas_unicos_sin_na=drop_col (df_reservas_unicos_sin_na, 'arrival_date_day_of_month')
        df_reservas_unicos_sin_na=drop_col (df_reservas_unicos_sin_na, 'arrival_date_str')
        
        
        print(df_reservas_unicos_sin_na.info())
        print("\n")
        print(df_usuarios_unicos.info())

        # LOAD

        date_load_field(df_usuarios_unicos, 'df_usuarios')
        date_load_field(df_reservas_unicos_sin_na, 'df_reservas')



    etl()


if __name__ == "__main__":
    main()

