# hdp-enernoc-project
```
DESCRIBE FORMATTED all_sites;
```

Téléchargement des fichiers de données
```
wget https://open-enernoc-data.s3.amazonaws.com/anon/all-data.tar.gz
tar -xvzf all-data.tar.gz
```

Load des fichiers dans Hadoop
```
hadoop fs -put /tmp/Data /apps/hive/warehouse/
```

Création de la base
````
CREATE DATABASE enernoc;
```

Chargement des fichiers de données brutes dans 2 tables temporaires
```
USE enernoc;

DROP TABLE IF EXISTS enernoc_raw_data;
DROP TABLE IF EXISTS enernoc_data_tmp;
DROP TABLE IF EXISTS enernoc_data;
DROP TABLE IF EXISTS enernoc_sites;


CREATE EXTERNAL TABLE enernoc_raw_data (
    time_stamp TIMESTAMP,
    dttm_utc DATE,
    value FLOAT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION 'hdfs://sandbox-hdp.hortonworks.com:8020/apps/hive/warehouse/enernoc/data'
TBLPROPERTIES ('skip.header.line.count' = '1');

CREATE TABLE enernoc_data_tmp AS SELECT 
    CAST(REGEXP_EXTRACT(INPUT__FILE__NAME, '([0-9]*)\.csv$', 1) AS INT) AS site_id,
    time_stamp,
    dttm_utc,
    value
FROM enernoc_raw_data;

CREATE EXTERNAL TABLE enernoc_sites ( 
    site_id INT,
    industry STRING,
    sub_industry STRING,	
    sq_ft STRING,
    lat FLOAT,
    lng FLOAT,
    time_zone STRING,
    tz_offset STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION 'hdfs://sandbox-hdp.hortonworks.com:8020/apps/hive/warehouse/enernoc/sites'
TBLPROPERTIES ('skip.header.line.count' = '1');
```

```
DESCRIBE FORMATTED enernoc_raw_data;
DESCRIBE FORMATTED enernoc_data_tmp;
DESCRIBE FORMATTED enernoc_sites;
```

Création de la table définitive nous permettant d'effectuer l'ensemble des requêtes demandées
```
CREATE TABLE enernoc_data AS SELECT
	enernoc_data_tmp.site_id,
	enernoc_data_tmp.dttm_utc,
	enernoc_data_tmp.value,
	enernoc_sites.industry,
	enernoc_sites.sub_industry,
	enernoc_sites.sq_ft,
	enernoc_sites.lat,
	enernoc_sites.lng,
	enernoc_sites.time_zone,
	enernoc_sites.tz_offset,
	CASE
           WHEN concat(date_format(enernoc_data_tmp.dttm_utc, 'MM'), date_format(enernoc_data_tmp.dttm_utc, 'dd')) BETWEEN '0321' AND '0620' then 'Spring'
           WHEN concat(date_format(enernoc_data_tmp.dttm_utc, 'MM'), date_format(enernoc_data_tmp.dttm_utc, 'dd')) BETWEEN '0621' AND '0920' then 'Summer'
           WHEN concat(date_format(enernoc_data_tmp.dttm_utc, 'MM'), date_format(enernoc_data_tmp.dttm_utc, 'dd')) BETWEEN '0921' AND '1220' then 'Autumn'
           ELSE 'Winter'
	END AS season
FROM enernoc_data_tmp JOIN enernoc_sites
ON (enernoc_data_tmp.site_id = enernoc_sites.site_id);
```

CdC totale des 100 sites (pas de temps 5 minutes)
```
SELECT
 	dttm_utc,
	ROUND(SUM(value),2) AS value
FROM enernoc_data
GROUP BY (dttm_utc);
```

CdC moyenne par secteur d’activité (pas de temps 5 minutes)
```
SELECT
 	industry AS industry,
	dttm_utc,
 	ROUND(AVG(value),2) AS cdc_moyenne
FROM enernoc_data
GROUP BY industry, dttm_utc;
```

CdC totale des 100 sites (pas de temps hebdomadaire)
```
SELECT
	WEEKOFYEAR(dttm_utc) AS week,
	ROUND(SUM(value),2) AS value
FROM enernoc_data
GROUP BY WEEKOFYEAR(dttm_utc);
```

CdC moyenne par secteur d’activité (pas de temps hebdomadaire)
```
SELECT
	industry,
	ROUND(AVG(value),2) AS value
FROM enernoc_data
GROUP BY industry;
```

intensité énergétique (ratio energie/surface) : au total, puis par saison (hiver, printemps, été, automne)

by industry
```
SELECT
	industry AS industry,
 	SUM(value)/SUM(sq_ft) AS ratio
FROM enernoc_data
GROUP BY industry
ORDER BY ratio DESC;

SELECT
	industry AS industry,
 	season AS season,
 	SUM(value)/SUM(sq_ft) AS ratio
FROM enernoc_data
GROUP BY industry, season
ORDER BY ratio DESC;
```

by industry and sub-industry
```
SELECT
	industry AS industry,
	sub_industry AS sub_industry,
 	SUM(value)/SUM(sq_ft) AS ratio
FROM enernoc_data
GROUP BY industry, sub_industry
ORDER BY ratio DESC;

SELECT
	industry AS industry,
	sub_industry AS sub_industry,
 	season AS season,
 	SUM(value)/SUM(sq_ft) AS ratio
FROM enernoc_data
GROUP BY industry, sub_industry, season
ORDER BY ratio DESC;
```

Journée la plus énergivore par site
```
SELECT DISTINCT
 	enernoc_data.site_id,
 	DATE_FORMAT(enernoc_data.dttm_utc, 'dd/MM/yyyy'),
 	max_value.value
FROM 
	(SELECT
		enernoc_data.site_id,
		MAX(enernoc_data.value) as value
	FROM enernoc_data
	GROUP BY enernoc_data.site_id
	)
	max_value, 
	enernoc_data
WHERE
	max_value.site_id = enernoc_data.site_id
	and max_value.value = enernoc_data.value
ORDER BY value DESC;
```

