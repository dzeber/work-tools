#######################################################################
###  
###  Some utility functions for working with Rmarkdown.
###  
#######################################################################


##----------------------------------------------------------------------------
##
## Parameters
##
## Rmarkdown allows for setting general script parameters in the YAML header.
## It is often also desirable to define constants early in the code that
## parametrize the analyses. Depending on the context, some of these are better
## set in an initial code block, and some in the YAML header, whichever allows
## for best interpretability.
##
## Parameters specified in the YAML header are made available in a read-only
## list 'params' in the global environment. We allow for an additional parameter
## list 'codeparams' which is to be initialized once and will be read-only from
## then on.
##
## When accessing parameter values, both lists are queried, and 'params' values
## override values in 'codeparams'.

## Initialize the 'codeparams' read-only list in the global environment.
initializeParams <- function(...) {
    codeparams <- list(...)
    assign("codeparams", codeparams, envir = globalenv())
    lockBinding("codeparams", globalenv())
}

## Retrieve the value for the given parameter name, if any. Search both 'params'
## and 'codeparams', with the former taking precedence.
getParam <- function(param) {
   val <- NULL
   if(exists("params")) val <- params[[param]]
   if(is.null(val) && exists("codeparams")) val <- codeparams[[param]]
   val
}


##----------------------------------------------------------------------------
##

## Data files are typically stored in a separate directory. To avoid having to
## deal with this explicitly in the code, the relative data dir path can be set
## as the param 'data_path'.
##
## Given the name of a data file, return the path the file in the data dir.
## If no data path is specified, default to the current dir.
dataPath <- function(datafile = "") {   
    datapath <- getParam("data_path")
    if(is.null(datapath)) {
        datafile
    } else {
        file.path(datapath, datafile)
    }
}

## Wrapper for inline Rmarkdown code chunks (currently needs to be called
## manually).
## If the code fails, eg. because a variable is not yet defined, print some
## placeholder text.
## Optionally specify a vector of chunk names that the inline code depends on.
## Their caches will be loaded prior to evaluating the chunk.
## Currently, the cache dir needs to be specified explicitly.
## If the cache is not lazy-loadable, caching will store an RData rather than
## an rdb. This function has the option to load the RData in that case, but
## does not do it by default, since the RData will likely get loaded anyway
## when the chunk is evaluated.
inline <- function(expr, dependent_chunks = NULL, load_rdata = FALSE) {
    tryCatch({
        cache_dir <- opts_chunk$get("cache.path")
        if(!is.null(dependent_chunks) && !is.null(cache_dir)) {
            ## The cache file will be '.rdb', or '.RData' if cache.lazy is
            ## FALSE. Check for both.
            cache_dbs <- list.files(cache_dir, pattern = "\\.(rdb|RData)$")
            cached <- unlist(lapply(dependent_chunks, function(dc) {
                chunk_caches <- grep(sprintf("^%s", dc), cache_dbs,
                    value = TRUE)
                chunk_rdb <- grep("\\.rdb$", chunk_caches, value = TRUE)
                if(length(chunk_rdb) > 0) chunk_rdb else chunk_caches
            }))
            for(cf in cached) {
                if(grepl("\\.rdb$", cf)) {
                    lazyLoad(file.path(cache_dir, sub("\\.rdb$", "", cf)),
                        envir = globalenv())
                } else {
                    load(file.path(cache_dir, cf), envir = globalenv())
                }
            }
        }
        expr
    }, error = function(e) {
        errmsg <- as.character(e)
        placeholder_template <- "__\\*\\*%s\\*\\*__"
        placeholder_msg <- if(isTRUE(grepl("object .+ not found", errmsg))) {
            ## Failed because required object is not yet defined.
            "R inline (pending)"
        } else {
            ## Failed because of an error in the code.
            "R inline: ERROR"
        }
        sprintf(placeholder_template, placeholder_msg)
    })
}

## List and remove objects in the workspace (global env) larger than a certain
## threshold.
## This is useful for cleaning up at the end of rendering an Rmd, as sometimes
## memory usage from large data tables causes problems.
## 'maxsize_mb' is the maximum size in MB for which an object will be kept -
## larger objects will be purged. For simplicity, we use 1 MB = 1e6 bytes.
## By default, objects exceeding the threshold are listed but not removed.
## To actually remove them, set purge = TRUE.
purgeLargeObjects <- function(maxsize_mb = 100, reallypurge = FALSE) {
    logcutoff <- log10(maxsize_mb) + 6
    objs <- ls(globalenv())
    objsizes <- as.numeric(lapply(objs, function(objnm) {
        object.size(get(objnm, envir = globalenv()))
    }))
    largeobjs <- log10(objsizes) > logcutoff

    if(!any(largeobjs)) {
        message("No large objects to purge.")
    } else {
        topurge <- objs[largeobjs]
        message(sprintf("Large objects to be purged: %s\n",
            paste(topurge, collapse = ", ")))
        if(reallypurge) {
            rm(list = topurge, envir = globalenv())
            invisible(gc())
        }
    }
}

