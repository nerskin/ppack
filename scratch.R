library(dplyr)
library(tidyverse)
library(duckdb)
library(RSQLite)
library(devtools)
library(arrow)
load_all()
document()
unlink('scratch.duckdb')
unlink('scratch.sl3')
con <- dbConnect(duckdb(),"scratch.duckdb")
dbExecute(con,"pragma memory_limit='4GB';")

con_sqlite <- dbConnect(SQLite(),'scratch.sl3')

RSQLite:::extension_load(con_sqlite@ptr,'distlib/distlib_64.so','sqlite3_distlib_init')

copy_to(con,historical_figures)

historical_db <- tbl(con,'historical_figures') |>
  mutate(unique_id = row_number())

passes <- list('surname'='surname','dob'='dob','postcode_fake'='postcode_fake','first_names'='first_name',c('occupation','gender'))

comparison_ids <- extract_blocks(historical_db,historical_db,'unique_id','unique_id',passes) %>%
  filter(unique_id_left < unique_id_right) %>%
  copy_to(con,.)

#comparison_ids <- copy_to(con_duckdb,comparison_ids)
#historical_db <- copy_to(con_duckdb,historical_db)
#dbDisconnect(con)

comparison_spec <- c('first_name'='first_name','dob'='dob','surname'='surname','postcode_fake'='postcode_fake')

compute_pairs_dataset(historical_db,historical_db,'unique_id','unique_id',comparison_ids) -> res

comparator_expressions <-  add_expression_comparators(dob_comparison=if_else(dob_left==dob_right,"agree","disagree"),
                                                      gender_comparison = case_when(
                                                        gender_left == "male" & gender_right == "male" ~ "agree (male)",
                                                        gender_left == "female" & gender_right == "female" ~ "agree (female)",
                                                        !gender_left %in% c('female','male') | !gender_right %in% c('female','male') ~ NA,
                                                        TRUE ~ "disagree"))


comparator_expressions <- c(build_exact_comparator_expressions(c('birth_place','occupation','postcode_fake')),comparator_expressions,build_jw_comparator_expressions(c('first_name','surname')))

res <- compute_pairs_dataset(historical_db,historical_db,'unique_id_left','unique_id_right',comparison_ids)

res |>
  transmute(!!!comparator_expressions) |>
  count(!!!syms(names(comparator_expressions))) |>
  collect() -> pattern_counts

total_pairs <- nrow(collect(historical_db))^2/2

calculate_u_probs <- function(con,data_A,data_B,comparator_expressions,size=1e6){
  data_A_rows <- count(data_A) |> pull(n)
  data_B_rows <- count(data_B) |> pull(n)


  data_A <- mutate(data_A,synthetic_id_left = row_number()) |>
    add_suffix('_left')
  data_B <- mutate(data_B,synthetic_id_right = row_number()) |>
    add_suffix('_right')

  pairs <- tibble(synthetic_id_left = sample(seq_len(data_A_rows),size=size,replace=TRUE),
                  synthetic_id_right = sample(seq_len(data_B_rows),size=size,replace=TRUE))

  pairs <- copy_to(con,df=pairs,name='_pairs_temp',overwrite = TRUE)

  sample_pairs <- pairs |>
    left_join(data_A) |>
    left_join(data_B)

  u_probs <- sample_pairs |>
    transmute(!!!comparator_expressions) |>
    pivot_longer(cols = everything()) |>
    group_by(name) |>
    count(value) |>
    filter(!is.na(value)) |>
    mutate(p=n/(sum(n)+0.0))#0.0 is to trigger type coercion

  u_probs <- u_probs |>
    group_by(name) |>
    collect()

  res_names <- group_keys(u_probs) |>
    pluck(1)

  u_probs |>
    group_split() |>
    map(\(x)select(x,-name,-n)) |>
    map(\(x) set_names(x$p,x$value)) |>
    set_names(res_names)

}

u_probs <- calculate_u_probs(con,historical_db,historical_db,comparator_expressions=comparator_expressions,size=1e6)

document()
load_all()
run_em(pattern_counts,maxiter=20,u_probabilities = u_probs,total_pairs = total_pairs) -> em_results

attach(em_results)


weights_df <- tibble(variable = names(weights),val = weights) |>
  mutate(val = map(val,\(x) tibble(level=names(x),weight=x))) |>
  unnest(cols=c(val))

weights_df <- copy_to(dest=res$src,df=weights_df,overwrite=TRUE)

comparisons_wide <- res |>
  transmute(unique_id_left,unique_id_right,!!!comparator_expressions) |>
  compute()

comparisons_long <- comparisons_wide |>
  pivot_longer(cols = -starts_with('unique_id'),names_to='variable',values_to = 'level') |>
  compute()

comparisons_long |>
  left_join(weights_df,by=c('variable','level')) |>
  mutate(weight = if_else(!is.na(weight),weight,0)) |>
  group_by(unique_id_left,unique_id_right) |>
  summarise(weight=sum(weight)) |>
  ungroup() |>
  filter(weight > 0)
