##############################################################
### 
###  Utility R code
### 
##############################################################


## Order the rows of a data frame over one or more columns. 
## Specify the df and the column names. 

order.df = function(df, ..., decreasing=FALSE, na.last=TRUE) {
    cols = list(...)
    cols = lapply(cols, function(nc) { unlist(df[[nc]]) })
    df = df[do.call(order, c(cols, decreasing=decreasing, na.last=na.last)),]
    rownames(df) = NULL
    df
}


## Apply a composition of functions to a list. 
## X - an object as in lapply.
## 
## FUN - single function or list of functions (from innermost to outermost) 
## to be composed and applied to elements of X.
##   * Eg. if FUN = list(f, g, h), returns lapply(X, function(r) { h(g(f(r))) }).
##
## ... - additional arguments to be passed to functions in FUN. 
##   * If a single function is supplied to FUN, any arguments passed in ... 
##     will be supplied to FUN, as in lapply.
##   * If FUN contains multiple functions, the i-th argument supplied in ... 
##     will be passed to FUN[[i]]. 
##     In this case, if the i-th argument is a list, its elements will be treated 
##     as mutliple arguments for FUN[[i]]. 
##     If any single arguments should themselves be lists, wrap them in a list. 
##   * If m = length(...) < length(FUN), the arguments will be supplied to 
##     the first m functions in FUN, and no additional arguments will be passed 
##     to the remaining functions. 
##   * If the i-th element of ... is NULL, this will be taken to mean that 
##     no arguments are to be passed to FUN[[i]]. 
##     To pass a single unnamed argument of NULL, use list().
##

lcapply = function(X, FUN, ...) {
    if(!is.list(FUN)) {
        if(!is.function(FUN))
            stop("FUN must be either a list or a function")
        
        ## FUN is a function.
        if(missing(ARGS)) {
            if(missing(...))
                return(lapply(X, FUN))
            
            
        } else {
            return(do.call(lapply, c(list(X = X, FUN = FUN), as.list(ARGS))))    
        }
        ## TODO
    }
    ## FUN is a list. 
    if(!missing(ARGS)) {
        if(!is.list(ARGS) || length(ARGS) != length(FUN))
            stop("ARGS must be a list the same length as FUN")
    
        ## Wrap additional args into functions. 
        FUN = lapply(seq_along(FUN), function(i) { 
            f = eval(bquote(function(r) { 
                    do.call(.(f), .(a))
                }, list(f = FUN[[i]], a = c(quote(r), as.list(ARGS[[i]])))))
            # function(r) { do.call(FUN[[i]], c(r, as.list(ARGS[[i]]))) }
            ## Don't need local index i. 
            environment(f) = parent.env(environment(f))
            f
        })
    }
         
    ## Compose functions. 
    cfun = function(xval) {
        Reduce(function(r, f) { f(r) }, FUN, init = xval, right = FALSE)
    }
    ef = new.env(parent = parent.env(environment(cfun)))
    assign("FUN", FUN, ef)
    environment(cfun) = ef
    
    lapply(X, cfun)
}

