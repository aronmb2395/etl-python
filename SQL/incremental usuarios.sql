-- drop table tmp_usuarios_recarga;
-- drop table temp_hist_usuarios;



-- Carga incremental sobre la tabla hist_usuarios por campo id

DO $$
DECLARE
  max_id INTEGER;
  recarga_incremental TEXT;
BEGIN
  -- seleccionamos max id
  SELECT MAX(id) INTO max_id FROM public.hist_usuarios;

  -- Crear una tabla temporal para almacenar las nuevas filas
  CREATE TEMP TABLE temp_hist_usuarios AS TABLE public.hist_usuarios WITH NO DATA;

  -- query dinamica para poder usar el valor de la variable max_id
  recarga_incremental := format(
    'COPY public.hist_usuarios (id, name, username, email, phone, website, address_street, address_suite, address_city, address_zipcode, address_geo_lat, address_geo_lng, company_name, company_catchphrase, company_bs, date_load, date_load_time) FROM ''C:/Users/aronm/Documents/Proyecto ETL/df_usuarios.CSV'' DELIMITER '','' CSV HEADER WHERE id > %s;',
    max_id
  );

  -- Ejecutamos la query dinamica
  EXECUTE recarga_incremental;

  -- Insertar las nuevas filas en la tabla temporal
  INSERT INTO temp_hist_usuarios
  SELECT * FROM public.hist_usuarios WHERE id > max_id;
END $$;




-- Comprobamos los nuevos registros

select * from temp_hist_usuarios 


-- Creamos tabla temporal tmp_usuarios_recarga para popular los nuevos registros en las tablas company y address

CREATE TEMP TABLE tmp_usuarios_recarga (
  id INT4 PRIMARY KEY,  
  name VARCHAR(50),
  username VARCHAR(50),
  email VARCHAR(50),
  phone VARCHAR(50),
  website VARCHAR(50),
  address_street VARCHAR(50),
  address_suite VARCHAR(50),
  address_city VARCHAR(50),
  address_zipcode VARCHAR(50),
  address_geo_lat FLOAT,
  address_geo_lng FLOAT,
  company_name VARCHAR(50),
  company_catchPhrase VARCHAR(50),
  company_bs VARCHAR(50),
  date_load DATE,
  date_load_time INT
);

INSERT INTO tmp_usuarios_recarga 
	SELECT *
	FROM  hist_usuarios;



-- Añadimos el valor de id_company a los nuevos registros en la tabla tmp_usuarios_recarga

ALTER TABLE tmp_usuarios_recarga ADD COLUMN id_company INT;

UPDATE tmp_usuarios_recarga AS us
SET id_company = ust.id_company
FROM (
    SELECT DISTINCT 
        ROW_NUMBER() OVER (ORDER BY date_load ASC) AS id_company, 
        company_name
    FROM tmp_usuarios_recarga
) AS ust
WHERE us.company_name = ust.company_name;



-- Añadimos el valor de id_address a los nuevos registros en la tabla tmp_usuarios_recarga

ALTER TABLE tmp_usuarios_recarga ADD COLUMN id_address INT;

UPDATE tmp_usuarios_recarga AS us
SET id_address = ads.id_address
FROM (
    SELECT DISTINCT
        ROW_NUMBER() OVER () AS id_address,
        address_city,
        address_zipcode
    FROM tmp_usuarios_recarga
) AS ads
WHERE us.address_city = ads.address_city AND us.address_zipcode = ads.address_zipcode;


-- Comprobamos que se hayan añadido bien estos valores en la tabla tmp_usuarios_recarga
select * from tmp_usuarios_recarga

	
-- Realizamos la carga incremental en company solo de los registros nuevos añadidos a hist_usuarios
	
INSERT INTO company (id, company_name, company_catchphrase, company_bs)
SELECT id_company, company_name, company_catchphrase, company_bs
FROM tmp_usuarios_recarga
WHERE EXISTS (
  SELECT 1
  FROM temp_hist_usuarios
  WHERE temp_hist_usuarios.id = tmp_usuarios_recarga.id
);

-- Realizamos la carga incremental en address solo de los registros nuevos añadidos a hist_usuarios
	
INSERT INTO address (id, address_city, address_street, address_suite, address_zipcode, address_geo_lat, address_geo_lng)
SELECT  id_address, address_city, address_street, address_suite, address_zipcode, address_geo_lat, address_geo_lng
FROM tmp_usuarios_recarga
WHERE EXISTS (
  SELECT 1
  FROM temp_hist_usuarios
  WHERE temp_hist_usuarios.id = tmp_usuarios_recarga.id
);


-- Eliminamos los campos ya no necesarios en tmp_usuarios_recarga para poder realizar recarga incremental en usuarios

ALTER TABLE tmp_usuarios_recarga
DROP COLUMN "company_name", 
DROP COLUMN "company_catchphrase",
DROP COLUMN "company_bs",
DROP COLUMN "address_city", 
DROP COLUMN "address_street",
DROP COLUMN "address_suite",
DROP COLUMN "address_zipcode", 
DROP COLUMN "address_geo_lat",
DROP COLUMN "address_geo_lng"


-- Realizamos la recarga incremental en usuarios solo de los registros nuevos añadidos a hist_usuarios
	
INSERT INTO usuarios 
	SELECT * 
	FROM  tmp_usuarios_recarga
	WHERE id > (SELECT MAX(id) FROM usuarios);

-- Comprobamos
select * from usuarios;


-- Guardamos las tablas 

COPY usuarios TO 'C:/Users/aronm/Documents/Proyecto ETL/tablas_exportadas/usuarios.CSV' DELIMITER ',' CSV HEADER;
COPY address TO 'C:/Users/aronm/Documents/Proyecto ETL/tablas_exportadas/address.CSV' DELIMITER ',' CSV HEADER;
COPY company TO 'C:/Users/aronm/Documents/Proyecto ETL/tablas_exportadas/company.CSV' DELIMITER ',' CSV HEADER;

