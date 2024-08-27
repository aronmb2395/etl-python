
/***************************************************************************************************************
****************************************************************************************************************
								CREACIÓN TABLAS Y RELACIONES
****************************************************************************************************************
****************************************************************************************************************/

-- Creamos tabla ususarios e importamos datos de fichero df_usuarios generado con python

CREATE TABLE hist_usuarios (
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


copy public.hist_usuarios (id, name, username, email, phone, website, address_street, address_suite, address_city, address_zipcode, address_geo_lat, address_geo_lng, company_name, company_catchphrase, company_bs, date_load, date_load_time) FROM 'C:/Users/aronm/Documents/Proyecto ETL/df_usuarios.CSV' DELIMITER ',' CSV HEADER  ;


CREATE TEMP TABLE tmp_usuarios (
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

INSERT INTO tmp_usuarios 
	SELECT *
	FROM  hist_usuarios;


-- Comprobamos tabla usuarios

select * from tmp_usuarios;

-- Creamos id_company para normalizar esta tabla y tener separada la información de compañías con datos de ususario

ALTER TABLE tmp_usuarios ADD COLUMN id_company INT;

UPDATE tmp_usuarios AS us
SET id_company = ust.id_company
FROM (
    SELECT DISTINCT 
        ROW_NUMBER() OVER (ORDER BY date_load ASC) AS id_company, 
        company_name
    FROM tmp_usuarios
) AS ust
WHERE us.company_name = ust.company_name;


-- Creamos id_address para normalizar esta tabla y tener separada la información de direcciones con datos de ususario

ALTER TABLE tmp_usuarios ADD COLUMN id_address INT;

UPDATE tmp_usuarios AS us
SET id_address = ads.id_address
FROM (
    SELECT DISTINCT
        ROW_NUMBER() OVER () AS id_address,
        address_city,
        address_zipcode
    FROM tmp_usuarios
) AS ads
WHERE us.address_city = ads.address_city AND us.address_zipcode = ads.address_zipcode;

select * from tmp_usuarios;

-- Creamos estructura de tabla company
CREATE TABLE company (
  id INT PRIMARY KEY,
  company_name VARCHAR(50),
  company_catchphrase VARCHAR(50),
  company_bs VARCHAR(50)
);


-- Creamos estructura de tabla address
CREATE TABLE address (
	id INT PRIMARY KEY,
	address_city VARCHAR(50),
 	address_street VARCHAR(50),
  	address_suite VARCHAR(50),
  	address_zipcode VARCHAR(50),
	address_geo_lat FLOAT,
	address_geo_lng FLOAT
);

-- Populamos tabla company con valores de tabla usuarios
INSERT INTO company (id, company_name, company_catchphrase, company_bs)
SELECT id_company, company_name, company_catchphrase, company_bs
FROM tmp_usuarios;

SELECT * FROM company;


-- Populamos tabla address con valores de tabla usuarios
INSERT INTO address (id, address_city, address_street, address_suite, address_zipcode, address_geo_lat, address_geo_lng)
SELECT id_address, address_city, address_street, address_suite, address_zipcode, address_geo_lat, address_geo_lng
FROM tmp_usuarios;

SELECT * FROM address
	order by id asc;




-- Eliminamos campos de compañía tabla ususarios y establecemos id_company foreign key de id tabla company

ALTER TABLE tmp_usuarios
DROP COLUMN "company_name", 
DROP COLUMN "company_catchphrase",
DROP COLUMN "company_bs",
DROP COLUMN "address_city", 
DROP COLUMN "address_street",
DROP COLUMN "address_suite",
DROP COLUMN "address_zipcode", 
DROP COLUMN "address_geo_lat",
DROP COLUMN "address_geo_lng";



CREATE TABLE usuarios (
  id INT4 ,  
  name VARCHAR(50),
  username VARCHAR(50),
  email VARCHAR(50),
  phone VARCHAR(50),
  website VARCHAR(50),
  date_load DATE,
  date_load_numeric INT,
  id_company INT,
  id_address INT
);


INSERT INTO usuarios 
	SELECT *
	FROM  tmp_usuarios;


ALTER TABLE usuarios 
ADD CONSTRAINT id_company FOREIGN KEY (id_company) REFERENCES company(id);
ALTER TABLE usuarios 
ADD CONSTRAINT id_address FOREIGN KEY (id_address) REFERENCES address(id);


	

-- Eliminamos campos de compañía tabla ususarios y establecemos id_company foreign key de id tabla company



SELECT * FROM usuarios;
	



-- Creamos tabla reservas

CREATE TABLE reservas (
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


-- Poblamos tabla reservas con valrores Reservas del fichero df_reservas de python
copy public.reservas (hotel,is_canceled,lead_time,stays_in_weekend_nights,stays_in_week_nights,adults,children,meal,country,is_repeated_guest,previous_cancellations,previous_bookings_not_canceled,reserved_room_type,assigned_room_type,id_agent,reservation_status,reservation_status_date,arrival_date,date_load, date_load_numeric) FROM 'C:/Users/aronm/Documents/Proyecto ETL/df_reservas.CSV' DELIMITER ',' CSV HEADER  ;


-- Añadimos carga incremental por campo max id

DO $$
DECLARE
  max_date_load INTEGER;
  recarga_incremental_reservas TEXT;
BEGIN
  -- seleccionamos max id
  SELECT MAX(date_load_numeric) INTO max_date_load FROM public.reservas;

  -- query dinamica para poder usar el valor de la variable max_id
  recarga_incremental_reservas := format(
    'copy public.reservas (hotel,is_canceled,lead_time,stays_in_weekend_nights,stays_in_week_nights,adults,children,meal,country,is_repeated_guest,previous_cancellations,previous_bookings_not_canceled,reserved_room_type,assigned_room_type,id_agent,reservation_status,reservation_status_date,arrival_date,date_load, date_load_numeric) FROM ''C:/Users/aronm/Documents/Proyecto ETL/df_reservas.CSV'' DELIMITER '','' CSV HEADER WHERE date_load_numeric > %s;',
    max_date_load
  );

  -- Ejecutamos la query dinamica
  EXECUTE recarga_incremental_reservas;
END $$;


select * from reservas;

-- Creamos tabla hotel

ALTER TABLE reservas
ADD COLUMN id_hotel VARCHAR(20);

UPDATE reservas
SET id_hotel =
  CASE
    WHEN hotel = 'City Hotel' THEN 'H2'
	WHEN hotel = 'Resort Hotel' THEN 'H1'
    ELSE NULL
  END;



-- Creamos estructura de tabla hotel
CREATE TABLE hotel (
  id VARCHAR(20) PRIMARY KEY,
  hotel_name VARCHAR(50)
);


-- Populamos tabla hotel con valores de tabla reservas
INSERT INTO hotel (id, hotel_name)
SELECT DISTINCT
	id_hotel, hotel
FROM reservas; 


SELECT * from hotel


--  Establecemos clave foránea y elminamos campos que ya repetidos en reservas

ALTER TABLE reservas
DROP COLUMN "hotel",
DROP COLUMN "date_load_numeric";

-- Añadimos clave foránea a la tabla de RESERVAS para que se una con tabla HOTEL y con la tabla USUARIOS
ALTER TABLE reservas
ADD CONSTRAINT id_hotel FOREIGN KEY (id_hotel) REFERENCES hotel(id),
ADD CONSTRAINT id_agent FOREIGN KEY (id_agent) REFERENCES usuarios(id);

select * from reservas


	
/***************************************************************************************************************
****************************************************************************************************************
								CONSULTAS SQL TRAS NORMALIZACIÓN TABLAS
****************************************************************************************************************
****************************************************************************************************************/



	
/****************************************************************************************************************
									 Número de reservas por hotel
****************************************************************************************************************/


SELECT 
	h.hotel_name,
	count(r.id) 	as num_reservas
	FROM hotel as h
	
	RIGHT JOIN reservas as r
	ON h.id = r.id_hotel
	GROUP BY  h.hotel_name
	ORDER BY num_reservas desc

/****************************************************************************************************************
  				Número de reservas ordenado ascendentemente por mes y año de 'arravial_date' 
****************************************************************************************************************/
	
select 
	TO_CHAR(arrival_date,'Mon') || '-' || TO_CHAR(arrival_date,'yyyy') AS Month_Year,
	count(id) 	as num_reservas
	from reservas 
	Group by Month_Year,extract(month from arrival_date)
	order by extract(month from arrival_date) asc

	
/****************************************************************************************************************
										TOP 3 menús según hotel
****************************************************************************************************************/
	
(SELECT
    h.hotel_name,
    r.meal,
    COUNT(r.meal) AS num_meal
FROM reservas as r
	LEFT JOIN hotel as h 
	on h.id = r.id_hotel
WHERE r.id_hotel = 'H1'
GROUP BY  h.hotel_name, r.meal
ORDER BY num_meal DESC
LIMIT 3)

UNION ALL

(SELECT
    h.hotel_name,
    r.meal,
    COUNT(r.meal) AS num_meal
FROM reservas as r
	LEFT JOIN hotel as h 
	on h.id = r.id_hotel
WHERE r.id_hotel = 'H2'
GROUP BY  h.hotel_name, r.meal
ORDER BY num_meal DESC
LIMIT 3);


/****************************************************************************************************************
			Reservas de clientes que cancelan, repiten y % repetidas sobre canceladas, según Agente
****************************************************************************************************************/


SELECT
	cp.name,
	cp.num_clientes_repiten,
	cc.num_clientes_cancelan,
	TO_CHAR((cp.num_clientes_repiten * 1.0 / cc.num_clientes_cancelan) * 100 ,'999D99%')  as repetidas_sobre_canceladas 
	
FROM
(
	-- Reservas de clientes que repiten, según Agente	
	SELECT
	r.id_agent,
	u.name,  
	count(r.id)		as num_clientes_repiten
	FROM reservas as r
	LEFT JOIN usuarios as u
	ON u.id = r.id_agent
	WHERE r.is_repeated_guest = 1
	GROUP BY r.id_agent, u.name
	ORDER BY num_clientes_repiten desc) as cp

	LEFT JOIN 
	(
	
	-- Reservas de clientes que cancelan, según Agente
	SELECT
	r.id_agent,
	u.name,  
	count(r.id)		as num_clientes_cancelan
	FROM reservas as r
	LEFT JOIN usuarios as u
	ON u.id = r.id_agent
	WHERE r.is_canceled = 1
	GROUP BY u.name, r.id_agent
	ORDER BY num_clientes_cancelan desc) AS cc

	ON cc.id_agent = cp.id_agent



/****************************************************************************************************************
 Número de reservas superiores a 1 reserva con estancia mayor a un semana (7 días) por compañía y país (no nulo)
****************************************************************************************************************/

SELECT
	cc.company_name,
	rr.country,
	SUM(rr.num_reservas) AS num_reservas
	
FROM
(SELECT
	r.id_agent,
	r.country,
	count(r.id)  as num_reservas
	from reservas as r
	where r.stays_in_weekend_nights > 7 and r.country is not null
	group by r.id_agent,r.country ) as rr

	LEFT JOIN 
	(SELECT
		c.company_name,
		u.id			as id_agent
		from usuarios as u
		left join company as c
		on c.id = u.id_company) as cc
		
	ON cc.id_agent = rr.id_agent

	GROUP BY 	cc.company_name, rr.country
	HAVING SUM(rr.num_reservas) > 1 
	ORDER BY num_reservas desc



/***************************************************************************************************************
****************************************************************************************************************
						
							EXPORTAMOS TABLAS A CSV PARA LECTURA POWER BI

NOTA: Es recomendable realizar la conexión directa entre PostgresSQL y PowerBI, no obstante, por motivos de
	replicación del ejercicio lo haremos mediante exportaciones de tablas en ficheros csv
	
****************************************************************************************************************
****************************************************************************************************************/



COPY usuarios TO 'C:/Users/aronm/Documents/Proyecto ETL/tablas_exportadas/usuarios.CSV' DELIMITER ',' CSV HEADER;
COPY reservas TO 'C:/Users/aronm/Documents/Proyecto ETL/tablas_exportadas/reservas.CSV' DELIMITER ',' CSV HEADER;
COPY address TO 'C:/Users/aronm/Documents/Proyecto ETL/tablas_exportadas/address.CSV' DELIMITER ',' CSV HEADER;
COPY company TO 'C:/Users/aronm/Documents/Proyecto ETL/tablas_exportadas/company.CSV' DELIMITER ',' CSV HEADER;
COPY hotel TO 'C:/Users/aronm/Documents/Proyecto ETL/tablas_exportadas/hotel.CSV' DELIMITER ',' CSV HEADER;
