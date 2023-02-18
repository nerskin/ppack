
#' Add suffix
#'
#' Add on a suffix to the columns of a data frame. Don't add the suffix if already there
#'
#' @param data a data-frame--ish object
#' @param suffix the suffix to add to all the colnames
#' @importFrom dplyr rename_with if_else
#' @importFrom stringr str_ends
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
add_suffix_char <- function(vec,suffix){
  if_else(str_ends(vec,suffix),vec,glue("{vec}{suffix}"))
}
