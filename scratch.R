```{sql, connection = con}
create table ctePreDrugTarget AS
SELECT d.drug_exposure_id
, d.person_id
, c.concept_id AS ingredient_concept_id
, d.drug_exposure_start_date AS drug_exposure_start_date
, d.days_supply AS days_supply
, COALESCE(
  ---NULLIF returns NULL if both values are the same, otherwise it returns the first parameter
  NULLIF(drug_exposure_end_date, NULL),
  ---If drug_exposure_end_date != NULL, return drug_exposure_end_date, otherwise go to next case
  NULLIF(CAST(STRFTIME('%s', DATETIME(drug_exposure_start_date, 'unixepoch', (days_supply)||' days')) AS REAL), drug_exposure_start_date),
  ---If days_supply != NULL or 0, return drug_exposure_start_date + days_supply, otherwise go to next case
  CAST(STRFTIME('%s', DATETIME(drug_exposure_start_date, 'unixepoch', (1)||' days')) AS REAL)
  ---Add 1 day to the drug_exposure_start_date since there is no end_date or INTERVAL for the days_supply
) AS drug_exposure_end_date
FROM main.drug_exposure d
JOIN main.concept_ancestor ca ON ca.descendant_concept_id = d.drug_concept_id
JOIN main.concept c ON ca.ancestor_concept_id = c.concept_id
WHERE c.vocabulary_id =  CAST('RxNorm' as TEXT) AND c.concept_class_id = 'Ingredient'
AND d.drug_concept_id != 0 ---Our unmapped drug_concept_id's are set to 0, so we don't want different drugs wrapped up in the same era
AND coalesce(d.days_supply,0) >= 0 ---We have cases where days_supply is negative, and this can set the end_date before the start_date, which we don't want. So we're just looking over those rows. This is a data-quality issue.
AND d.person_id = 1
```




```{r}
dbGetQuery(con, "select a.*, b.concept_name as ingredient_name 
           from ctePreDrugTarget a
           join concept b on a.ingredient_concept_id = b.concept_id
           order by drug_exposure_start_date") %>% 
  restore_dates()
```




```{r}
ctePreDrugTarget <- tribble(
  ~drug_exposure_id, ~person_id, ~ingredient_concept_id, ~drug_exposure_start_date, ~drug_exposure_end_date,
  1,                1,         1125315,               1,                        4,
  2,                1,         1125315,               2,                        5,
  3,                1,         1125315,               7,                        9,
)

ctePreDrugTarget
```



```{r}
restore_dates <- function(df) {
  dplyr::mutate(df, dplyr::across(dplyr::matches("date$"),
                                  ~as.Date(as.POSIXct(., origin = "1970-01-01")))) %>% 
    select(-matches("datetime"))
}
```



```{r}

dbGetQuery(con, 
           "select a.*, b.concept_name as drug_concept_name 
           from main.drug_exposure a
           join concept b on a.drug_concept_id = b.concept_id
           where person_id = 1
           order by drug_exposure_start_date") %>% 
  restore_dates() %>% 
  select(drug_concept_name, everything())
```
