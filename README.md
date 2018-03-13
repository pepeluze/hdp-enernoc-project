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
CREATE TABLE enernoc_data_sites AS SELECT
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


