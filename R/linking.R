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
#' @export
extract_blocks <- function(data_A,data_B,blocking_variables){
  #lifted_join_by <- purrr::lift(dplyr::join_by)#a version of join_by that can take a vector rather than dots
  passes <- purrr::map(blocking_variables,\(x)dplyr::inner_join(data_A,data_B,x,suffix=c('_left','_right')))
  ## the next bit is horrible and needed only because the keep argument of inner_join is not supported by inner_join.tbl_lazy
  passes <- purrr::map2(passes,blocking_variables,\(x,y){
    bind_cols(x,
    purrr::map(y,\(z){
      transmute(x,"{z}_left" := .data[[z]],"{z}_right" := .data[[z]]) #|> ## see https://rlang.r-lib.org/reference/glue-operators.html
    }) |>
      reduce(bind_cols)
  )})
  all_comparison_pairs <- purrr::reduce(passes,dplyr::union)
  all_comparison_pairs
}
