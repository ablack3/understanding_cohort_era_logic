union_using_windows <- function(con, cdm_schema) {
  
  tibble::tibble(DBI::dbGetQuery(con, glue::glue("
  WITH cte1 AS (
    SELECT DISTINCT 
      person_id, 
      MIN(drug_exposure_start_date) OVER (PARTITION BY person_id, drug_exposure_end_date) AS start_dt,
      MAX(coalesce(drug_exposure_end_date, drug_exposure_start_date)) OVER (PARTITION BY person_id, drug_exposure_start_date) AS end_date
    FROM {cdm_schema}.drug_exposure
    WHERE drug_exposure_start_date IS NOT NULL
  ),
  cte2 AS (
    SELECT DISTINCT person_id, start_dt, end_date,
      CASE WHEN 
          (start_dt NOT BETWEEN COALESCE( lag(start_dt) OVER(PARTITION BY person_id ORDER BY end_date), DATEADD(day, 1, end_date)) AND COALESCE( lag(end_date) OVER(PARTITION BY person_id ORDER BY end_date), DATEADD(day, 1, end_date))) AND 
          (start_dt NOT BETWEEN COALESCE(lead(start_dt) OVER(PARTITION BY person_id ORDER BY end_date), DATEADD(day, 1, end_date)) AND COALESCE(lead(end_date) OVER(PARTITION BY person_id ORDER BY end_date), DATEADD(day, 1, end_date))) AND
          (start_dt NOT BETWEEN COALESCE( lag(start_dt) OVER(PARTITION BY person_id ORDER BY start_dt), DATEADD(day, 1, end_date)) AND COALESCE( lag(end_date) OVER(PARTITION BY person_id ORDER BY start_dt), DATEADD(day, 1, end_date))) AND 
          (start_dt NOT BETWEEN COALESCE(lead(start_dt) OVER(PARTITION BY person_id ORDER BY start_dt), DATEADD(day, 1, end_date)) AND COALESCE(lead(end_date) OVER(PARTITION BY person_id ORDER BY start_dt), DATEADD(day, 1, end_date)))
      THEN start_dt ELSE NULL END AS valid_start,
      CASE WHEN 
          (end_date NOT BETWEEN COALESCE( lag(start_dt) OVER(PARTITION BY person_id ORDER BY end_date), DATEADD(day, 1, end_date)) AND COALESCE( lag(end_date) OVER(PARTITION BY person_id ORDER BY end_date), DATEADD(day, 1, end_date))) AND 
          (end_date NOT BETWEEN COALESCE(lead(start_dt) OVER(PARTITION BY person_id ORDER BY end_date), DATEADD(day, 1, end_date)) AND COALESCE(lead(end_date) OVER(PARTITION BY person_id ORDER BY end_date), DATEADD(day, 1, end_date))) AND
          (end_date NOT BETWEEN COALESCE( lag(start_dt) OVER(PARTITION BY person_id ORDER BY start_dt), DATEADD(day, 1, end_date)) AND COALESCE( lag(end_date) OVER(PARTITION BY person_id ORDER BY start_dt), DATEADD(day, 1, end_date))) AND 
          (end_date NOT BETWEEN COALESCE(lead(start_dt) OVER(PARTITION BY person_id ORDER BY start_dt), DATEADD(day, 1, end_date)) AND COALESCE(lead(end_date) OVER(PARTITION BY person_id ORDER BY start_dt), DATEADD(day, 1, end_date)))
      THEN end_date ELSE NULL END AS valid_end
    FROM cte1 
  ), 
  cte3 AS (
   -- could also use a join instead of a subquery. Not sure which is better.
    SELECT DISTINCT 
      person_id, 
      MAX(valid_start) OVER (partition by person_id ORDER BY valid_start ROWS UNBOUNDED PRECEDING) AS start_date,
      valid_end AS end_date
    FROM cte2
  ) 
  SELECT * FROM cte3 WHERE end_date IS NOT NULL and start_date is not null -- is there a way to remove this last subquery?
  ")))
}


union_using_union_all <- function(con, cdm_schema) {
  tibble::tibble(DBI::dbGetQuery(con, glue::glue("
  with cteEndDates AS (
	SELECT
		person_id,
		event_date as end_date
	FROM
	(
		SELECT
			person_id,
			event_date,
			SUM(event_type) OVER (PARTITION BY person_id ORDER BY event_date ROWS UNBOUNDED PRECEDING) AS interval_status
		FROM
		(
			SELECT
				person_id,
				drug_exposure_start_date AS event_date,
				-1 AS event_type
			FROM {cdm_schema}.drug_exposure

			UNION ALL

			SELECT
				person_id,
				drug_exposure_end_date AS event_date,
				1 AS event_type
			FROM {cdm_schema}.drug_exposure
		) RAWDATA
	) e
	WHERE interval_status = 0
),
cteEnds AS
(
	SELECT 
		c.person_id,
		c.drug_exposure_start_date as start_date,
		MIN(e.end_date) AS end_date
	FROM {cdm_schema}.drug_exposure c
	INNER JOIN cteEndDates e ON c.person_id = e.person_id
		AND e.end_date >= c.drug_exposure_start_date
	GROUP BY  
		c.person_id,
		c.drug_exposure_start_date
)
select person_id, min(start_date) as start_date, end_date
from cteEnds
group by person_id, end_date
;
  ")))
}

con <- DBI::dbConnect(odbc::odbc(),
                      Driver   = Sys.getenv("SQL_SERVER_DRIVER"),
                      Server   = Sys.getenv("CDM5_SQL_SERVER_SERVER"),
                      Database = Sys.getenv("CDM5_SQL_SERVER_CDM_DATABASE"),
                      UID      = Sys.getenv("CDM5_SQL_SERVER_USER"),
                      PWD      = Sys.getenv("CDM5_SQL_SERVER_PASSWORD"),
                      TrustServerCertificate="yes",
                      Port     = 1433)

cdm_schema <- Sys.getenv("CDM5_SQL_SERVER_CDM_SCHEMA")
system.time({
df1 <- union_using_windows(con, cdm_schema)
})
system.time({
df2 <- union_using_union_all(con, cdm_schema)
})

con <- DBI::dbConnect(RPostgres::Redshift(),
                      dbname   = Sys.getenv("CDM5_REDSHIFT_DBNAME"),
                      host     = Sys.getenv("CDM5_REDSHIFT_HOST"),
                      port     = Sys.getenv("CDM5_REDSHIFT_PORT"),
                      user     = Sys.getenv("CDM5_REDSHIFT_USER"),
                      password = Sys.getenv("CDM5_REDSHIFT_PASSWORD"))

cdm_schema <- Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA")

DBI::dbDisconnect(con)

con <- dbConnect(RPostgres::Postgres(),
                 dbname = Sys.getenv("CDM5_POSTGRESQL_DBNAME"),
                 host = Sys.getenv("CDM5_POSTGRESQL_HOST"),
                 user = Sys.getenv("CDM5_POSTGRESQL_USER"),
                 password = Sys.getenv("CDM5_POSTGRESQL_PASSWORD"))

cdm_schema <- Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA")

res <- microbenchmark::microbenchmark(union_using_windows(con, cdm_schema), 
                                      union_using_union_all(con, cdm_schema),
                                      times = 10L)

print(res)

df1 <- df1 %>% dplyr::arrange(person_id, start_date, end_date)
df2 <- df2 %>% dplyr::arrange(person_id, start_date, end_date)

waldo::compare(df1, df2)


df0 <- DBI::dbGetQuery(con, glue::glue("select person_id, drug_exposure_start_date as start_date, drug_exposure_end_date as end_date from {cdm_schema}.drug_exposure")) %>% 
  dplyr::filter(person_id == 28) %>% 
  dplyr::arrange(person_id, start_date, end_date)






