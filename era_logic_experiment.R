



# idea

sqldf::sqldf("
  SELECT person_id, 
 
    case when 
      not(lag(start) < end < lag(end)) and not(lead(start) < end < lead(end)) order by end AND
      not(lag(start) < end < lag(end)) and not(lead(start) < end < lead(end)) order by start
    then end else null end AS valid_end
  
    case when 
      not(lag(start) < start < lag(start)) and not(lead(start) < start < lead(end)) order by end AND
      not(lag(end) < start < lag(start)) and not(lead(start) < start < lead(end)) order by start
    start start_date else null end AS valid_start
  
  FROM intervals
  ")


# demorgan

DBI::dbGetQuery(con, "
  SELECT person_id, 
    case when not(
        (end_date between  lag(start_dt) over(partition by person_id order by end_date) AND  lag(end_date) over(partition by person_id order by end_date)) OR 
        (end_date between lead(start_dt) over(partition by person_id order by end_date) AND lead(end_date) over(partition by person_id order by end_date)) OR
        (end_date between  lag(start_dt) over(partition by person_id order by start_dt) AND  lag(end_date) over(partition by person_id order by start_dt)) OR 
        (end_date between lead(start_dt) over(partition by person_id order by start_dt) AND lead(end_date) over(partition by person_id order by start_dt))
    ) then end_date else null end AS valid_end,
  
    case when not(
        (start_dt between  lag(start_dt) over(partition by person_id order by end_date) AND  lag(end_date) over(partition by person_id order by end_date)) OR 
        (start_dt between lead(start_dt) over(partition by person_id order by end_date) AND lead(end_date) over(partition by person_id order by end_date)) OR
        (start_dt between  lag(start_dt) over(partition by person_id order by start_dt) AND  lag(end_date) over(partition by person_id order by start_dt)) OR 
        (start_dt between lead(start_dt) over(partition by person_id order by start_dt) AND lead(end_date) over(partition by person_id order by start_dt))
    ) then start_dt else null end AS valid_start
  FROM intervals
  ")



con <- DBI::dbConnect(duckdb::duckdb())

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
  "equals",       2,                     2,           "2022-01-05",       "2022-01-10") %>%
   dplyr::mutate(dplyr::across(dplyr::matches("date"), as.Date))




dplyr::tbl(con, "intervals") %>% 
  dplyr::arrange(start_dt)




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
  ),
  cte3 AS (
   -- could also use a join instead of a subquery. Not sure which is better.
    SELECT DISTINCT 
      person_id, 
      MAX(valid_start) OVER (partition by person_id ORDER BY valid_start ROWS UNBOUNDED PRECEDING) AS start_dt,
      valid_end AS end_date
    FROM cte2
  ) 
  SELECT * FROM cte3 WHERE end_date IS NOT NULL
  ")))
}

con <- DBI::dbConnect(duckdb::duckdb())
purrr::walk(c("precedes", "meets", "overlaps", "finished_by", "contains", "starts", "equals"), ~union(con, .))
purrr::walk(c( "overlaps"), ~union(con, .))
purrr::walk(c(NA), ~union(con, .))
DBI::dbDisconnect(con, shutdown = T)


purrr::walk(c( "finished_by","starts"), ~union(con, .))


dplyr::mutate(dplyr::across(dplyr::matches("date"), as.Date)) %>% 
  dplyr::filter(relationship %in% c("reference", "overlaps"))# %>% 
# dplyr::filter(definition_id == 1, person_id == 1)


DBI::dbWriteTable(con, "intervals", intervals, overwrite = T)


# closer
DBI::dbGetQuery(con, "
  SELECT person_id, start_dt, end_date,
    case when 
        (start_dt NOT BETWEEN  lag(start_dt) over(partition by person_id order by end_date) AND  lag(end_date) over(partition by person_id order by end_date)) AND 
        (start_dt NOT BETWEEN lead(start_dt) over(partition by person_id order by end_date) AND lead(end_date) over(partition by person_id order by end_date)) AND
        (start_dt NOT BETWEEN  lag(start_dt) over(partition by person_id order by start_dt) AND  lag(end_date) over(partition by person_id order by start_dt)) AND 
        (start_dt NOT BETWEEN lead(start_dt) over(partition by person_id order by start_dt) AND lead(end_date) over(partition by person_id order by start_dt))
    then start_dt else null end AS valid_start,
  
    case when
        not(end_date NOT BETWEEN  lag(start_dt) over(partition by person_id order by end_date) AND  lag(end_date) over(partition by person_id order by end_date)) AND 
        not(end_date NOT BETWEEN lead(start_dt) over(partition by person_id order by end_date) AND lead(end_date) over(partition by person_id order by end_date)) AND
        not(end_date NOT BETWEEN  lag(start_dt) over(partition by person_id order by start_dt) AND  lag(end_date) over(partition by person_id order by start_dt)) AND 
        not(end_date NOT BETWEEN lead(start_dt) over(partition by person_id order by start_dt) AND lead(end_date) over(partition by person_id order by start_dt))
    then end_date else null end AS valid_end
  FROM intervals
  ")


sqldf::sqldf("
select distinct
  definition_id, person_id, start, end, 
  sum (
    NOT(lag(start) < end   < lag(end)   OR lead(start) < end   < lead(end)) + 
    NOT(lag(start) < end   < lag(end) OR lead(start) < end   < lead(end)) + 
  ) over(partition by definition_id, person_id, start order by end)
  +
  sum (
    16 * NOT(lag(start) < start < lag(end) OR lead(start) < start < lead(end))
    2 * NOT(lag(start) < start < lag(start) OR lead(start) < start < lead(end))
  ) over(partition by person_id, start order by start)
 as boolean_data
from intervals
")



con <- DBI::dbConnect(duckdb::duckdb())
dbWriteTable(con, "intervals", intervals)

DBI::dbGetQuery(con, "
SELECT DISTINCT
  definition_id, person_id, start_date, end_date,
    case when NOT (
           LAG(start_date) OVER (PARTITION BY definition_id, person_id ORDER BY end_date) < end_date AND end_date <  LAG(end_date) OVER (PARTITION BY definition_id, person_id ORDER BY end_date)
          OR
          LEAD(start_date) OVER (PARTITION BY definition_id, person_id ORDER BY end_date) < end_date AND end_date < LEAD(end_date) OVER (PARTITION BY definition_id, person_id ORDER BY end_date)
    ) then 1 else 0 end
   +
    case when NOT (
           LAG(start_date) OVER (PARTITION BY definition_id, person_id ORDER BY end_date) < start_date AND start_date <  LAG(end_date) OVER (PARTITION BY definition_id, person_id ORDER BY end_date)
          OR
          LEAD(start_date) OVER (PARTITION BY definition_id, person_id ORDER BY end_date) < start_date AND start_date < LEAD(end_date) OVER (PARTITION BY definition_id, person_id ORDER BY end_date)
    ) then 2 else 0 end as n2
from intervals

  ;
             
             ")

# ) OVER (PARTITION BY definition_id, person_id, start_date ORDER BY end_date) AS N