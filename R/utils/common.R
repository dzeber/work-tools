#######################################################################
###  
###  Convenience functions for use on hala or locally.
###  
#######################################################################


## Wrap referenced objects into a list.
## List names are the corresponding object names, 
## unless custom names are supplied.
param.list <- function(..., .names) {
    a <- substitute(list(...))[-1]
    a <- unlist(lapply(a, as.character))
    an <- a
    if(!missing(.names) && !is.null(names(.names))) {
        rn <- an %in% names(.names)
        if(any(rn)) {
            an[rn] <- .names[an[rn]]
        }
    }
    setNames(lapply(a, get), an)
}

## Convert a vector of numbers to their relative percentages 
## of the total.
## Rounded to the specified number of decimals.
## To skip rounding, use decimals = NULL.
to.pct <- function(x, decimals = 3) {
    pct <- x / sum(x) * 100
    if(is.null(decimals)) return(pct)
    round(pct, decimals)
}

## Trim whitespace from both ends of string. 
trim <- function(x) {
    gsub("^\\s+|\\s+$", "", x)
}

## Add commas to long numbers for easier reading in a printed data table.
## (This converts them to character).
longnum <- function(x) {
    prettyNum(x, big.mark = ",", scientific = FALSE)
}


