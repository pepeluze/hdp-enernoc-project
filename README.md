# hdp-enernoc-project

Chargement des fichiers de données brutes dans 2 tables temporaires
```
USE enernoc;

DROP TABLE IF EXISTS enernoc_data_tmp;
DROP TABLE IF EXISTS enernoc_data;

CREATE EXTERNAL TABLE enernoc_data_tmp (
    time_stamp TIMESTAMP,
    dttm_utc DATE,
    value FLOAT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION 'hdfs://sandbox-hdp.hortonworks.com:8020/apps/hive/warehouse/enernoc/data'
TBLPROPERTIES ('skip.header.line.count' = '1');

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
Création de la table des données
```
CREATE TABLE enernoc_data AS SELECT
  CAST(REGEXP_EXTRACT(INPUT__FILE__NAME, '([0-9]*)\.csv$', 1) AS INT) AS site_id,
	enernoc_data.dttm_utc,
	enernoc_data.value,
	enernoc_sites.industry,
	enernoc_sites.sub_industry,
	enernoc_sites.sq_ft,
	enernoc_sites.lat,
	enernoc_sites.lng,
	enernoc_sites.time_zone,
	enernoc_sites.tz_offset,
	CASE
           WHEN concat(date_format(dttm_utc, 'MM'), date_format(dttm_utc, 'dd')) BETWEEN '0321' AND '0620' then 'Spring'
           WHEN concat(date_format(dttm_utc, 'MM'), date_format(dttm_utc, 'dd')) BETWEEN '0621' AND '0920' then 'Summer'
           WHEN concat(date_format(dttm_utc, 'MM'), date_format(dttm_utc, 'dd')) BETWEEN '0921' AND '1220' then 'Autumn'
           ELSE 'Winter'
END AS season
FROM enernoc_data JOIN enernoc_sites
ON (enernoc_data.site_id = enernoc_sites.site_id);
```
CdC totale des 100 sites (pas de temps 5 minutes)
```
CREATE TABLE cdc_5_mins_all_sites AS SELECT
 	dttm_utc, ROUND(SUM(value),2)
FROM enernoc_data
GROUP BY (dttm_utc)
ORDER BY (dttm_utc);
```

CREATE TABLE cdc_ratio_total AS SELECT
 	industry AS industry,
 	AVG(value/sq_ft) AS ratio,
 	ROUND(SUM(value),2) AS value
FROM enernoc_data
GROUP BY industry
ORDER BY industry;

SELECT
 	sub_industry AS sub_industry,
 	SUM(value)/SUM(sq_ft) AS ratio,
 	ROUND(SUM(value),2) AS value
FROM enernoc_data
GROUP BY sub_industry
ORDER BY ratio;

intensité énergétique (ratio energie/surface) : au total, puis par saison (hiver, printemps, été, automne)
```
CREATE TABLE cdc_ratio_total_by_season AS SELECT
	industry AS industry,
 	season AS season,
 	AVG(value/sq_ft) AS ratio,
 	ROUND(SUM(value),2) AS value,
FROM enernoc_data
GROUP BY industry, season
ORDER BY industry;

SELECT
	industry AS industry,
 	season AS season,
 	AVG(value/sq_ft) AS ratio,
 	ROUND(SUM(value),2) AS value
FROM enernoc_data
GROUP BY industry, season
ORDER BY ratio desc;



SELECT
 	site_id,
 	dttm_utc,
 	max_value.value
FROM 
	(SELECT
		site_id,
		MAX(value) as value
	FROM enernoc_data
	GROUP BY site_id
	)
	max_value, 
	enernoc_data
WHERE
	max_value.site_id = enernoc_data.site_id
	and max_value.value = enernoc_data.value
GROUP BY site_id
ORDER BY max_value;
```

