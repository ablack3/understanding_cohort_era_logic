union <- function(con, rel) {
  intervals <- tibble::tribble( 
    ~relationship,  ~definition_id,        ~person_id,  ~start_dt,             ~end_date,
    "reference",    1,                     1,           "2022-01-05",       "2022-01-10",
    "precedes",     1,                     1,           "2022-01-01",       "2022-01-03",
    "meets",        1,                     1,           "2022-01-01",       "2022-01-04",
    "overlaps",     1,                     1,           "2022-01-01",       "2022-01-05",
    "finished_by",  1,                     1,           "2022-01-01",       "2022-01-10",
    "contains",     1,                     1,           "2022-01-01",       "2022-01-15",
    "starts",       1,                     1,           "2022-01-05",       "2022-01-07",
    "equals",       1,                     1,           "2022-01-05",       "2022-01-10",
    "reference",    1,                     2,           "2022-01-05",       "2022-01-10",
    "precedes",     1,                     2,           "2022-01-01",       "2022-01-03",
    "meets",        1,                     2,           "2022-01-01",       "2022-01-04",
    "overlaps",     1,                     2,           "2022-01-01",       "2022-01-05",
    "finished_by",  1,                     2,           "2022-01-01",       "2022-01-10",
    "contains",     1,                     2,           "2022-01-01",       "2022-01-15",
    "starts",       1,                     2,           "2022-01-05",       "2022-01-07",
    "equals",       1,                     2,           "2022-01-05",       "2022-01-10",
    "reference",    2,                     1,           "2022-01-05",       "2022-01-10",
    "precedes",     2,                     1,           "2022-01-01",       "2022-01-03",
    "meets",        2,                     1,           "2022-01-01",       "2022-01-04",
    "overlaps",     2,                     1,           "2022-01-01",       "2022-01-05",
    "finished_by",  2,                     1,           "2022-01-01",       "2022-01-10",
    "contains",     2,                     1,           "2022-01-01",       "2022-01-15",
    "starts",       2,                     1,           "2022-01-05",       "2022-01-07",
    "equals",       2,                     1,           "2022-01-05",       "2022-01-10",
    "reference",    2,                     2,           "2022-01-05",       "2022-01-10",
    "precedes",     2,                     2,           "2022-01-01",       "2022-01-03",
    "meets",        2,                     2,           "2022-01-01",       "2022-01-04",
    "overlaps",     2,                     2,           "2022-01-01",       "2022-01-05",
    "finished_by",  2,                     2,           "2022-01-01",       "2022-01-10",
    "contains",     2,                     2,           "2022-01-01",       "2022-01-15",
    "starts",       2,                     2,           "2022-01-05",       "2022-01-07",
    "equals",       2,                     2,           "2022-01-05",       "2022-01-10") |> 
    dplyr::mutate(dplyr::across(dplyr::matches("date"), as.Date)) |> 
    dplyr::filter(relationship %in% c("reference", rel)) 
  
  DBI::dbWriteTable(con, "intervals", intervals, overwrite = T)
  
  cli::cat_rule(glue::glue("Testing 'x' {rel} 'reference'"))
  cli::cat_line("**INPUT**")
  print(dplyr::tbl(con, "intervals"))
  cli::cat_line("**OUTPUT**")
  
  print(tibble::tibble(DBI::dbGetQuery(con, "
  WITH cte1 AS (
    SELECT DISTINCT 
      person_id, 
      MIN(start_dt) OVER (PARTITION BY person_id, end_date) AS start_dt,
      MAX(end_date) OVER (PARTITION BY person_id, start_dt) AS end_date
    FROM intervals
  ),
  cte2 AS (
    SELECT DISTINCT person_id, start_dt, end_date,
      CASE WHEN 
          (start_dt NOT BETWEEN COALESCE( lag(start_dt) OVER(PARTITION BY person_id ORDER BY end_date), end_date + 1) AND COALESCE( lag(end_date) OVER(PARTITION BY person_id ORDER BY end_date), end_date + 1)) AND 
          (start_dt NOT BETWEEN COALESCE(lead(start_dt) OVER(PARTITION BY person_id ORDER BY end_date), end_date + 1) AND COALESCE(lead(end_date) OVER(PARTITION BY person_id ORDER BY end_date), end_date + 1)) AND
          (start_dt NOT BETWEEN COALESCE( lag(start_dt) OVER(PARTITION BY person_id ORDER BY start_dt), end_date + 1) AND COALESCE( lag(end_date) OVER(PARTITION BY person_id ORDER BY start_dt), end_date + 1)) AND 
          (start_dt NOT BETWEEN COALESCE(lead(start_dt) OVER(PARTITION BY person_id ORDER BY start_dt), end_date + 1) AND COALESCE(lead(end_date) OVER(PARTITION BY person_id ORDER BY start_dt), end_date + 1))
      THEN start_dt ELSE NULL END AS valid_start,
      CASE WHEN 
          (end_date NOT BETWEEN COALESCE( lag(start_dt) OVER(PARTITION BY person_id ORDER BY end_date), end_date + 1) AND COALESCE( lag(end_date) OVER(PARTITION BY person_id ORDER BY end_date), end_date + 1)) AND 
          (end_date NOT BETWEEN COALESCE(lead(start_dt) OVER(PARTITION BY person_id ORDER BY end_date), end_date + 1) AND COALESCE(lead(end_date) OVER(PARTITION BY person_id ORDER BY end_date), end_date + 1)) AND
          (end_date NOT BETWEEN COALESCE( lag(start_dt) OVER(PARTITION BY person_id ORDER BY start_dt), end_date + 1) AND COALESCE( lag(end_date) OVER(PARTITION BY person_id ORDER BY start_dt), end_date + 1)) AND 
          (end_date NOT BETWEEN COALESCE(lead(start_dt) OVER(PARTITION BY person_id ORDER BY start_dt), end_date + 1) AND COALESCE(lead(end_date) OVER(PARTITION BY person_id ORDER BY start_dt), end_date + 1))
      THEN end_date ELSE NULL END AS valid_end
    FROM cte1 
  )
  SELECT DISTINCT a.person_id, a.valid_start AS start_dt, MIN(b.valid_end) AS end_date
  FROM cte2 a JOIN cte2 b on a.person_id = b.person_id
  WHERE a.valid_start <= b.valid_end
  GROUP BY a.person_id, a.valid_start
  ")))
}

con <- DBI::dbConnect(duckdb::duckdb())
purrr::walk(c("precedes", "meets", "overlaps", "finished_by", "contains", "starts", "equals"), ~union(con, .))
DBI::dbDisconnect(con, shutdown = T)
