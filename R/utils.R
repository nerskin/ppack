
#' Add suffix
#'
#' Add on a suffix to the columns of a data frame. Don't add the suffix if already there
#'
#' @param data a data-frame--ish object
#' @param suffix the suffix to add to all the colnames
#' @importFrom dplyr rename_with if_else
#' @importFrom stringr str_ends
#' @export
add_suffix <- function(data,suffix){
  data_new_names <- data |>
    rename_with(.fn= function(x){
      if_else(str_ends(x,suffix),x,glue("{x}{suffix}"))
    }
    )
  data_new_names
}

#' @importFrom stringr str_ends
#' @importFrom dplyr if_else
#' @importFrom glue glue
#' @export
add_suffix_char <- function(vec,suffix){
  if_else(str_ends(vec,suffix),vec,glue("{vec}{suffix}")) |>
    as.character()
}


#' Use the trick descripted at https://gregorygundersen.com/blog/2020/02/09/log-sum-exp/ to compute log(sum(exp(x))) in a numerically stable way
#' @param x a vector of numbers to compute the LSE of
#' @export
log_sum_exp <- function(x){
  max(x) + log(sum(exp(x - max(x))))
}
