-- drop table tmp_reservas_recarga;
-- drop table temp_hist_reservas;


-- Añadimos carga incremental por campo max_date_load

DO $$
DECLARE
  max_date_load INTEGER;
  recarga_incremental_reservas TEXT;
BEGIN
  -- seleccionamos max id
  SELECT MAX(date_load_numeric) INTO max_date_load FROM public.hist_reservas;

  -- Crear una tabla temporal para almacenar las nuevas filas
  CREATE TEMP TABLE temp_hist_reservas AS TABLE public.hist_reservas WITH NO DATA;

  -- query dinamica para poder usar el valor de la variable max_id
  recarga_incremental_reservas := format(
    'copy public.hist_reservas (hotel,is_canceled,lead_time,stays_in_weekend_nights,stays_in_week_nights,adults,children,meal,country,is_repeated_guest,previous_cancellations,previous_bookings_not_canceled,reserved_room_type,assigned_room_type,id_agent,reservation_status,reservation_status_date,arrival_date,date_load,date_load_numeric) FROM ''C:/Users/aronm/Documents/Proyecto ETL/df_reservas.CSV'' DELIMITER '','' CSV HEADER WHERE date_load_numeric > %s;',
    max_date_load
  );

  -- Ejecutamos la query dinamica
  EXECUTE recarga_incremental_reservas;

  -- Insertar las nuevas filas en la tabla temporal
  INSERT INTO temp_hist_reservas
	  SELECT * FROM public.hist_reservas WHERE date_load_numeric > max_date_load;

END $$;


-- Comprobamos nuevos registros en temp_hist_reservas

select * from temp_hist_reservas ;


-- Creamos tabla temporal tmp_reservas_recarga que nos servirá para definir las relaciones y las tablas de reservas 

CREATE TEMP TABLE tmp_reservas_recarga (
	id SERIAL PRIMARY KEY,
	hotel VARCHAR(50),
	is_canceled INT4,
	lead_time INT4,
	stays_in_weekend_nights INT4,
	stays_in_week_nights INT4,
	adults INT4,
	children INT4,
	meal VARCHAR(50),
	country VARCHAR(50),
	is_repeated_guest INT4,
	previous_cancellations INT4,
	previous_bookings_not_canceled INT4,
	reserved_room_type VARCHAR(50),
	assigned_room_type VARCHAR(50),
	id_agent INT4,
	reservation_status VARCHAR(50),
	reservation_status_date DATE,
	arrival_date DATE,
	date_load DATE,
	date_load_numeric INT
);


INSERT INTO tmp_reservas_recarga 
	SELECT *
	FROM  hist_reservas;


-- Creamos tmp_reservas_recarga y esablecemos los valores de id_hotel

ALTER TABLE tmp_reservas_recarga
ADD COLUMN id_hotel VARCHAR(20);

UPDATE tmp_reservas_recarga
SET id_hotel =
  CASE
    WHEN hotel = 'City Hotel' THEN 'H2'
	WHEN hotel = 'Resort Hotel' THEN 'H1'
    ELSE NULL
  END;

-- Comprobamos valore de tmp_reservas_recarga
select * from tmp_reservas_recarga


--  Eliminamos campos innecesarios en tmp_reservas_recarga

ALTER TABLE tmp_reservas_recarga
DROP COLUMN "hotel",
DROP COLUMN "date_load_numeric";


-- Insertamos los nuevos valores en reservas

INSERT INTO public.reservas 
	SELECT *
	FROM tmp_reservas_recarga
WHERE EXISTS (
  SELECT 1
  FROM temp_hist_reservas
  WHERE temp_hist_reservas.id = tmp_reservas_recarga.id
);

-- comprobamos que se hayan insertado nuevos valores 
select * from public.reservas 
order by id desc







COPY reservas TO 'C:/Users/aronm/Documents/Proyecto ETL/tablas_exportadas/reservas.CSV' DELIMITER ',' CSV HEADER;
COPY hotel TO 'C:/Users/aronm/Documents/Proyecto ETL/tablas_exportadas/hotel.CSV' DELIMITER ',' CSV HEADER;
