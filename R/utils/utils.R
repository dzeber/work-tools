#######################################################################
###  
###  General convenience functions.
###  
#######################################################################


## Reverse a factor ordering - for use with coord_flip().
## To use inside qplot: `qplot(eval(frev(xvar)), ...)`.
frev <- function(v) {
    v <- substitute(v)
    substitute(factor(v, levels = rev(levels(v))))
}


## Convert a vector of logical or character to factor using any given labels:
## - if no labels are given, convert to factor directly (the values themselves
##   are used as the levels).
## - if labels is an unnamed character vector, convert to factor using these
##   as levels.
## - if labels is a named vector and vals is character, map vals to new labels
##   before converting.
## - if vals is boolean, `labels` should be of the form
##   c("TRUE" = "...", "FALSE" = "..."). The boolean values will be relabelled
##   and converted to factor.
factorize <- function(vals, labels = NULL) {
    if(length(labels) == 0)
        return(factor(vals))

    if(length(names(labels)) > 0)
        vals <- labels[as.character(vals)]
    factor(vals, levels = labels)
}


## Join together string args as a multiline string.
## If any of the args is itself a vector of strings, it will be joined with
## a single space separator.
## Also in ggplot.R
multiline <- function(...) {
    strs <- list(...)
    strs <- lapply(strs, function(s) {
        if(length(s) > 1) paste(s, collapse = " ") else s
    })
    paste(as.character(strs), collapse = "\n")
}


## Compute a 7-day moving average for time series data, where a resulting
## current value is the average over the past 7 days up to an including the
## current value.
## Note: need to make sure any missing days are populated with 0 first.
weeklyMA <- function(tseries) {
    as.numeric(filter(tseries, rep(1/7, 7), sides = 1))
}


## Quick summary view for data tables.
DTSummary <- function(DT) {
    data.table(
        colname = names(DT),
        type = sapply(DT, class),
        hasNA = as.logical(lapply(DT, function(d) any(is.na(d))))
    )
}

