#' @importFrom glue glue
#' @importFrom cli cli_ul cli_li cli_end
#' @import purrr
#' @import S7

print <- new_external_generic("base","print","x")


# add some validation rules here
#' @export
blocking_rules <- new_class("blocking_rules",properties = list(
  rules = class_list#list of expressions - add some validator checks
)
)

print_list_of_expressions <- function(list){
  unordered_list <- cli_ul()
  walk(list,\(x)cli_li(format(x)))
  cli_end(unordered_list)
}

method(print,blocking_rules) <- function(x){
  if (length(x@rules) == 0){
    cat("An empty blocking rule. Be careful! This blocking rule will return all pairs!","\n")
  }
  else{
    cat(glue("Blocking rule with {length(x@rules)} blocking conditions."),"\n")
    print_list_of_expressions(x@rules)
  }
}


#' @export
ppack_spec <- new_class("ppack_spec",properties = list(
  blocking_rules = blocking_rules
)
)

method(print,ppack_spec) <- function(x){
  cat("A linking specification.","\n\n")

  cat("The blocking rules are",'\n')
  print_list_of_expressions(x@blocking_rules@rules)
}

.onLoad <- function(libname, pkgname) {
  S7::external_methods_register()
}

