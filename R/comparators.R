

#' @export
build_jw_comparator_expressions <- function(vars){
  vars_left <- if (is.null(names(vars))) add_suffix_char(vars,'_left') else add_suffix_char(names(vars),'_right')
  vars_right <- add_suffix_char(vars,'_right')
  map2(syms(vars_left),syms(vars_right),\(x,y) expr(
    case_when(
      jaro_winkler_similarity(!!x,!!y) == 1 ~ 3,
      jaro_winkler_similarity(!!x,!!y) > 0.9 ~ 2,
      jaro_winkler_similarity(!!x,!!y) > 0.85 ~ 1,
      TRUE ~ 0))) |>
    set_names(vars)
}


#' @export
build_exact_comparator_expressions <- function(vars){
  vars_left <- if (is.null(names(vars))) add_suffix_char(vars,'_left') else add_suffix_char(names(vars),'_right')
  vars_right <- add_suffix_char(vars,'_right')
  map2(syms(vars_left),syms(vars_right),\(x,y) expr(if_else(!!x==!!y,1,0))) |>
    set_names(vars)
}
