#' Simple test function
#'
#' @param x A vector of numbers to get the cumulative sums of
#'
#' @return A vector of the cumulative sums of the input
#' @export
#'
#' @examples
#' acc(1:10)
acc <- function(x)purrr::accumulate(x,`+`)


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
extract_blocks <- function(data_A,data_B,unique_id_A,unique_id_B,blocking_variables){
  data_A <- data_A |>
    add_suffix('_left')
  data_B <- data_B |>
    add_suffix('_right')
  blocking_vars_right <- blocking_variables
  blocking_vars_left <- names(blocking_variables)
  pass_specs <- map(blocking_variables,\(vars){
    glue("{vars}_right") |>
    set_names(glue("{vars}_left"))})
  passes <- map(pass_specs,\(pass_spec){
    res <- inner_join(data_A,data_B,pass_spec,suffix=c('_left','_right'))
  })
  unique_id_A <- glue("{unique_id_A}_left")
  unique_id_B <- glue("{unique_id_B}_right")
  passes |>
    map(\(x)distinct(x,.data[[unique_id_A]],.data[[unique_id_B]])) |>
    reduce(union)
}

#' Calculate fellegi-sunter weights
#'
#' For now this only understands exact matches
#'
#' @param data_A some sort of data frame or tbl_lazy etc
#' @param data_B some sort of data frame or tbl_lazy etc
#' @param unique_id_A The unique id column for the first dataset
#' @param unique_id_B The unique id column for the second dataset
#' @param comparison_spec  liable to change, some specification of the comparisons to make
#' @param comparison_ids the ids to compare. This will usually be output from extract_blocks
#' @export
compute_weights <- function(data_A,data_B,unique_id_A,unique_id_B,comparison_spec,comparison_ids){
  data_A <- add_suffix(data_A,"_left")
  data_B <- add_suffix(data_B,"_right")
  unique_id_A <- add_suffix_char(unique_id_A,'_left')
  unique_id_B <- add_suffix_char(unique_id_B,'_right')

  joined_data <- comparison_ids |>
    left_join(data_A,by=unique_id_A) |>
    left_join(data_B,by=unique_id_B)

  ## actually use this tbl_lazy to compute the weights - will require some metaprogramming unless it can be done withing the "programming with dplyr" framework

}
