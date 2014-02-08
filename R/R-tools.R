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
## FUN -  single function or list of functions (from innermost to outermost) 
## to be composed and applied to elements of X.
##   Eg. if FUN = list(f, g, h), returns lapply(X, function(r) { h(g(f(r))) })
## ARGS - additional args to be passed to each function. 
##   A list of vectors or lists of args the same length as FUN.
##   ARGS[[i]] gets passed as additional parameters to FUN[[i]]. 

lcapply = function(X, FUN, ARGS) {
    if(!is.list(FUN)) {
        if(!is.function(FUN))
            stop("FUN must be either a list or a function")
        
        ## FUN is a function.
        if(missing(ARGS)) 
            return(lapply(X, FUN))
        else {
            return(do.call(lapply, c(list(X = X, FUN = FUN), as.list(ARGS))))
        }       
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
            ## Don't need local index. 
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

