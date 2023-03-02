

#' @export
build_jw_comparator_expressions <- function(vars){
  vars_left <- if (is.null(names(vars))) add_suffix_char(vars,'_left') else add_suffix_char(names(vars),'_right')
  vars_right <- add_suffix_char(vars,'_right')
  map2(syms(vars_left),syms(vars_right),\(x,y) expr(
    case_when(
      jaro_winkler_similarity(!!x,!!y) == 1 ~ "full agreement",
      jaro_winkler_similarity(!!x,!!y) > 0.9 ~ "strong partial agreement",
      jaro_winkler_similarity(!!x,!!y) > 0.85 ~ "weak partial agreement",
      TRUE ~ "no agreement"))) |>
    set_names(vars)
}


#' @export
build_exact_comparator_expressions <- function(vars){
  vars_left <- if (is.null(names(vars))) add_suffix_char(vars,'_left') else add_suffix_char(names(vars),'_right')
  vars_right <- add_suffix_char(vars,'_right')
  map2(syms(vars_left),syms(vars_right),\(x,y) expr(if_else(!!x==!!y,"agree","disagree"))) |>
    set_names(vars)
}
