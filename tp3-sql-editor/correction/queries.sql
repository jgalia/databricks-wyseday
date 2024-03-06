-- Databricks notebook source
-- what is the total count of rows of your XX_noaa_workflow table ? 235282
select count(*) from wyseday_databricks.default.jg_noaa_workflow ;

-- COMMAND ----------

-- how many stations are there ?  85
select count(distinct(STATION)) from wyseday_databricks.default.jg_noaa_workflow ;


-- COMMAND ----------

-- how many stations in paris ? 1
select distinct(NAME) from jg_noaa_workflow where NAME like '%PARIS%';

-- COMMAND ----------

-- what is the cumulative sum of PRCP (precipitation) in PARIS since the start of the data for the 2024-02-01 ? (don't use filter) 5504.900000000059
select sum(PRCP) OVER (partition by NAME order by DATE asc) as sum_prcp, PRCP, NAME, DATE from jg_noaa_workflow  where NAME like 'PARIS%' order by NAME, DATE desc;

-- COMMAND ----------

-- what is the total sum of precipitation in Paris in february 2024 ? 69,4
CREATE OR REPLACE TEMPORARY view temp_prcp_table as (
select sum(PRCP) OVER (partition by NAME, month(DATE), year(DATE) order by DATE asc) as sum_prcp, PRCP, NAME, DATE, month(DATE) as mois, year(DATE) as annee 
from jg_noaa_workflow  where NAME like 'PARIS%' order by NAME, DATE desc);

-- COMMAND ----------

select * from temp_prcp_table limit 30 ;

-- COMMAND ----------

-- More or less than 2023 ? 2024 > 2023
select * from temp_prcp_table where mois = 2 and annee = 2023 ;
