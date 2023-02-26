library(dplyr)
library(tidyverse)
library(duckdb)
library(RSQLite)
library(devtools)
load_all()
document()
unlink('scratch.duckdb')
con <- dbConnect(duckdb(),"scratch.duckdb")
#con <- dbConnect(SQLite(),'scratch.sl3')

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

compute_weights(historical_db,historical_db,'unique_id','unique_id',comparison_spec = comparison_spec,comparison_exprs=list(),comparison_ids) -> res

comparator_expressions <-  add_expression_comparators(dob_comparison=if_else(dob_left==dob_right,1,0))


comparator_expressions <- c(build_exact_comparator_expressions(c('birth_place','gender','occupation','postcode_fake')),comparator_expressions,build_jw_comparator_expressions(c('first_name','surname')))

res |>
  transmute(!!!comparator_expressions) |>
  count(!!!syms(names(comparator_expressions))) |>
  collect() -> pattern_counts

total_pairs <- nrow(collect(historical_db))^2/2

calculate_u_probs <- function(data_A,data_B,size=1e6){
  data_A_rows <- count(data_A) |> pull(n)
  data_B_rows <- count(data_B) |> pull(n)
  data_A_ids <- sample(seq_len(data_A_rows),replace=FALSE)
  data_B_ids <- sample(seq_len(data_B_rows),replace=FALSE)



  data_A <- mutate(data_A,synthetic_id = data_A_ids)
  data_B <- mutate(data_B,synthetic_id = data_B_ids)
  inner_join(data_A,data_B,by='synthetic_id')
}

document()
load_all()
run_em(pattern_counts,maxiter=20,total_pairs = total_pairs) -> em_results
