#' Extract pairs to compare
#'
#' @param data_A A tbl_duckdb_connection ideally, but other varieties of tbl_lazy will probably work too
#' @param data_B A tbl_duckdb_connection ideally, but other varieties of tbl_lazy will probably work too
#' @param blocking_variables A list representing the blocking variables. For now only support exact matches.
#' Eventually this should support other types of blocking, like blocking using common prefixes
#' @return A lazy tbl_duckdb_connection which contains the comparisons that we want to make.
#'
#' @importFrom glue glue
#' @importFrom purrr map reduce
#' @importFrom dplyr inner_join rename_with union
#' @importFrom rlang set_names
#' @export
extract_blocks <- function(data_A,data_B,unique_id_A,unique_id_B,blocking_variables,blocking_expressions){
  data_A <- data_A |>
    add_suffix('_left')
  data_B <- data_B |>
    add_suffix('_right')
  blocking_vars_right <- blocking_variables
  blocking_vars_left <- names(blocking_variables)
  pass_specs <- map(blocking_variables,\(vars){
    glue("{vars}_right") |>
    as.character() |>
    set_names(glue("{vars}_left"))})
  passes <- map(pass_specs,\(pass_spec){
    res <- inner_join(data_A,data_B,pass_spec,suffix=c('_left','_right'))
  })
  cartesian_product <- inner_join(data_A,data_B,suffix=c('_left','_right'),by=character())
  expression_passes <- map(blocking_expressions,\(x)filter(cartesian_product,!!x))
  passes <- c(passes,expression_passes)
  unique_id_A <- glue("{unique_id_A}_left")
  unique_id_B <- glue("{unique_id_B}_right")
  passes |>
    map(\(x)select(x,all_of(c(unique_id_A,unique_id_B)))) |>
    map(collect) |>
    bind_rows() |>
    distinct(.data[[unique_id_A]],.data[[unique_id_B]])
  }

#' Bring together all the variables in a single table
#'
#' @param data_A some sort of data frame or tbl_lazy etc
#' @param data_B some sort of data frame or tbl_lazy etc
#' @param unique_id_A The unique id column for the first dataset
#' @param unique_id_B The unique id column for the second dataset
#' @param comparison_spec  liable to change, some specification of the comparisons to make
#' @param comparison_ids the ids to compare. This will usually be output from extract_blocks
#' @export
compute_blocked_dataset <- function(data_A,data_B,unique_id_A,unique_id_B,comparison_ids){
  data_A <- add_suffix(data_A,"_left")
  data_B <- add_suffix(data_B,"_right")
  unique_id_A <- add_suffix_char(unique_id_A,'_left')
  unique_id_B <- add_suffix_char(unique_id_B,'_right')

  joined_data <- comparison_ids |>
    left_join(data_A,by=unique_id_A) |>
    left_join(data_B,by=unique_id_B)
  
  joined_data
}

#' Add comparator supplied as a user-supplied expression
#'
#' @export
add_expression_comparators <- function(...){
 enquos(...)
}


#' @param expr_list dplyr expressions definiting the comparison levels. These will usually be case_when or if_else statements. This must be _named_
#' @export
do_comparisons <- function(joined_data,expr_list){
  transmute(joined_data,!!!expr_list)
}

#' @importFrom rlang eval_tidy
#' @export
run_em <- function(pattern_counts,maxiter=10,total_pairs,u_probabilities = NULL){
  ## some calculations are underflowing - should maybe calculate weights and then convert to probabilities?
  comparison_names <- names(pattern_counts) |>
    keep(\(x)x!='n')
  comparison_levels <- map(comparison_names,\(x)pull(distinct(pattern_counts,.data[[x]]),.data[[x]])) |> #assumes that every level appears in the data at least once - can do better
        set_names(comparison_names) |>
        map(\(x)set_names(x,x)) |>
        map(\(x)sort(x))

  m_probabilities <- comparison_levels |>
    map(\(x) seq(0.1,0.8,length=length(x)) |> set_names(names(x)))
  log_m_probs <- m_probabilities |>
    map(log)
  log_u_probs <- u_probabilities |>
    map(log)

  lambda <- 1e-7
  log_lambda <- log(lambda)
  log_one_minus_lambda <- log(1-lambda)


  for (i in seq_len(maxiter)){
    ## build up expressions to use in computing the match probabilities
    loglik_given_m <- map(comparison_names,\(x){
      name_sym <- sym(x)
      m_probability <- expr( if (!is.na(!!name_sym))log(m_probabilities[[!!x]][!!name_sym]) else 0)
    }) |>
      reduce(\(x,y) expr(!!x + !!y))
    loglik_given_u <- map(comparison_names,\(x){
      name_sym <- sym(x)
      u_probability <- expr(if (!is.na(!!name_sym)) log(u_probabilities[[!!x]][!!name_sym]) else 0)
    }) |>
      reduce(\(x,y)expr(!!x + !!y))
  pattern_counts <- pattern_counts |>
    rowwise() |>
    mutate(loglik_given_m = !!loglik_given_m,
           loglik_given_u = !!loglik_given_u,
	   log_probability_est = log_lambda + loglik_given_m - log_sum_exp(c(log_lambda + loglik_given_m, log_one_minus_lambda + loglik_given_u)), ##fix the denominator
      prob_est = exp(log_probability_est))

  #this next term is the denominator of the update term on page 2 of the supp. materials
  # we only need to compute it once per iteration
  expected_total_matches <- sum(pattern_counts[['n']]*pattern_counts[['prob_est']])
  expected_total_nonmatches <- sum(pattern_counts[['n']] * (1 - pattern_counts[['prob_est']]))
  lambda <- expected_total_matches/total_pairs
  log_lambda <- log(expected_total_matches) - log(total_pairs)
  #update each m and u probability
  m_probabilities_old <- m_probabilities
  u_probabilities_old <- u_probabilities
  for (name in comparison_names){
    non_missing_indices <- which(!is.na(pattern_counts[[name]]))
    pattern_counts_temp <- pattern_counts[non_missing_indices,]
    for (l in names(m_probabilities[[name]])  ){
      m_probabilities[[name]][l] <- sum((pattern_counts_temp[['n']]*(pattern_counts_temp[[name]] == l)*pattern_counts_temp[['prob_est']]))/sum(pattern_counts_temp[['n']]*pattern_counts_temp[['prob_est']])
    }
    for (l in u_probabilities[[name]]){
     u_probabilities[[name]][l] <- sum((pattern_counts_temp[['n']]*(pattern_counts_temp[[name]] == l)*(1-pattern_counts_temp[['prob_est']])))/sum(pattern_counts_temp[['n']] * (1 - pattern_counts_temp[['prob_est']]))
    }
  }
  print(glue("This is iteration number {i}"))
  print(glue("The largest change in an m probability was {max(unlist(m_probabilities) - unlist(m_probabilities_old))}"))
  print(glue("The largest change in a u probability was {max(unlist(u_probabilities) - unlist(u_probabilities_old))}"))
  print(glue("The current value of lambda is {lambda}\n"))
  }

  ## ensure the two lists have the same ordering
  m_probabilities <- m_probabilities[order(names(m_probabilities))]
  u_probabilities <- u_probabilities[order(names(u_probabilities))]

  m_probabilities <- map(m_probabilities,\(x) x[order(names(x))])
  u_probabilities <- map(u_probabilities,\(x) x[order(names(x))])

  ## calculate Fellegi-Sunter weights
  weights <- map2(m_probabilities,u_probabilities,\(x,y)log(x) - log(y))

  list(pattern_counts=pattern_counts,m_probabilities=m_probabilities,u_probabilities=u_probabilities,lambda=lambda,total_pairs=total_pairs,weights=weights)
  ## basically two steps: for each agreement pattern, calculate the running estimate of the match probability
  ## then update each of the parameters
}
