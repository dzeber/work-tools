#######################################################################
###  
###  Convenience functions for use on hala.
###  
#######################################################################

## Sets up directories for working on hala. 
##
## Creates local subdir of home dir on hala and switches to it. 
## Returns function that will generate matching paths on hala 
## leading to specified subdirs. 
##
## Specify base name of working dir to create, as well as optional subpath 
## (relative to home dir). Default subpath is "fhr"; passing NULL makes 
## base.dir a direct subdir of the home dir.
## Also includes boolean flag indicating whether or not to return HDFS function. 
set.dir <- function(base.name, path = "fhr", hdfs.fun = TRUE) {
    subpath <- if(is.null(path)) base.name else file.path(path, base.name)
    local.path <- file.path("~", subpath)
        
    ## Create path on local if doesn't exist. 
    system(sprintf("mkdir -p %s", local.path))
    ## Switch to new base dir. 
    setwd(local.path)
    
    ## Create function to generate HDFS paths. 
    if(hdfs.fun) {
        eval(bquote(function(dir.name = "") {
            file.path(.(p), dir.name)
        }, list(p = file.path("/user", Sys.getenv("USER"), subpath))))
    }
}


## Converts RHIPE job output generated with rhcollect statements to a data table. 
## Optionally set names for the outputted reduce value list.
## Optionally apply a multiplier to the reduce value.
make.dt <- function(x, valnames = NULL, mult = NULL) {
    #require(data.table)
    ## If we have a multiplier, apply it to x[[i]][[2]].
    ## Assumes that x[[i]][[2]] is a numeric vector or scalar, not a list.
    if(!is.null(mult)) {
        x <- lapply(x, function(r) {
            r[[2]] <- r[[2]] * mult
            r
        })
    }
    ## If we have custom column names for the values, set them. 
    ## Otherwise, leave names unchanged.
    cf <- if(!is.null(valnames)) function(r) { 
        r2 <- setNames(as.list(r[[2]]), valnames)
        if(length(valnames) != length(r2)) 
            warning("Supplied value names vector is not the right length")
        c(as.list(r[[1]]), r2) 
    } else function(r) { 
        c(as.list(r[[1]]), as.list(r[[2]])) 
    }
    x <- lapply(x, cf)
    rbindlist(x)
}


## Extract values of type field in MR output.
get.types <- function(x, typefld = "type") {
    unique(sapply(x, function(r) { r[[1]][[typefld]] }))
}

## Spit output of MR job according to type field in the key.
split.types <- function(x, typefld = "type") {
    types <- sapply(x, function(r) { r[[1]][[typefld]] })
    split(x, types)
}


## Split MR output on type label field and bind each sublist into DT.
## Name for value/count columns in DT is given by valnames.
## Can be either a single character vector to be applied to all tables,
## or a list mapping type name to value names. 
## A multiplier can be applied to values to adjust for sampling.
split.tables <- function(x, valnames = "count", mult = NULL,
                            sample.rate = c("1pct", "5pct"), typefld = "type") {
    x <- split.types(x, typefld)
    
    ## Use sampling rate to find multiplier.
    ## If 'mult' is given explicitly, it takes precedence.
    if(is.null(mult) && !missing(sample.rate)) {
        if(!sample.rate %in% c("1pct", "5pct"))
            stop("Sample rate value was not recognized")
        mult <- c("1pct" = 100, "5pct" = 20)[[sample.rate]]
    }
    
    if(!is.list(valnames)) {
        lapply(x, function(y) {
            y <- make.dt(y, valnames, mult)
            y[, eval(typefld) := NULL]
            # if(!is.null(mult)) {
                # y[, eval(valnames) := get(valnames) * mult]
            # }
            y
        })
    } else {
        nmx <- names(x)
        setNames(lapply(nmx, function(nn) {
            vn <- valnames[[nn]]
            d <- make.dt(x[[nn]], vn, mult)
            d[, eval(typefld) := NULL]
            # if(!is.null(mult)) {
                # d[, eval(vn) := get(vn) * mult]
            # }
            d
        }), nmx)
    }    
}

## Summing reducer that maintains counts of final records 
## by a named field in the reduce key.
## The idea is to record the final sizes of multiple data tables generated
## in a single job.
tablesummer <- function(tablefield = NULL) {
    count <- if(!is.null(tablefield)) {
        bquote(if(identical(.rhipe.current.state, "reduce") && 
                                .(tablefld) %in% names(reduce.key)) {
            rhcounter("TABLE_SIZES", reduce.key[[.(tablefld)]], 1)
        }, list(tablefld = tablefield))
    } else { NULL }
    bquote(expression(
        pre = { .sum <- 0 },
        reduce = {  .sum <- .sum + sum(unlist(reduce.values), na.rm = TRUE) },
        post = {
             rhcollect(reduce.key, .sum)
             .(increment)
         }
     ), list(increment = count))
}

## Rbind reducer that binds rows into data tables rather than data frames.
## Need to call library(data.table) in the reduce phase of setup.
DTrbinder <- expression(
    pre = { adata <- list() }, 
    reduce = { adata[[length(adata) + 1]] <- reduce.values },
    post = { 
        adata <- rbindlist(unlist(adata, recursive = FALSE))
        rhcollect(reduce.key, adata)
    }
)

