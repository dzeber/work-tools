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
##   * If the i-th element of ... is NULL or an empty list, this will be taken to mean that 
##     no arguments are to be passed to FUN[[i]]. 
##
## UNLIST - whether or not unlist should be applied to the result.
##

lcapply = function(X, FUN, ..., UNLIST = FALSE) {
    ## Check that FUN contains valid functions. 
    ## If not, generate more precise error message. 
    if(is.list(FUN)) {
        FUN = lapply(seq_along(FUN), function(i) {
            tryCatch(match.fun(FUN[[i]]), error = function(e) { e })
        })
        errs = sapply(FUN, function(f) { "error" %in% class(f) })
        if(any(errs)) {
            stop(paste(c("Errors were caused by elements of FUN:",
                unlist(lapply(which(errs), function(i) { 
                    sprintf("In FUN[[%s]]: %s", i, FUN[[i]]$message)
                }))), collapse = "\n"))
        }
        ## FUN contains 1 or more valid functions. 
    } else {
        FUN = tryCatch(match.fun(FUN), error = function(e) { e })
        if("error" %in% class(FUN))
            stop(sprintf("Error caused by FUN: \n%s", FUN$message))
    }
    
    if(is.list(FUN) && length(FUN) == 1)
        FUN = FUN[[1]]
    ## FUN is either a single function or a list of multiple functions. 
    
    res = if(!is.list(FUN)) {
        ## FUN is a single function.
        ## Perform lapply.
        lapply(X, FUN, ...)
    } else {
        ## Multiple functions.  
        if(!missing(...)) {
            ## Incorporate any specified args. 
            arglist = list(...)
            
            ## Cannot have more args than functions. 
            if(length(arglist) > length(FUN))
                stop("Cannot supply more args than there are functions in FUN")
            
            if(length(arglist) < length(FUN))
                arglist[(length(arglist) + 1) : length(FUN)] = list(NULL)
         
            ## Wrap additional args into functions, if any. 
            FUN = lapply(seq_along(FUN), function(i) { 
                a = arglist[[i]]
                if(is.null(a) || (is.list(a) && length(a) == 0))
                    return(FUN[[i]])
                
                if(!is.list(a))
                    a = list(a)
                    
                f = eval(bquote(function(r) { 
                        do.call(.(f), .(a))
                    }, list(f = FUN[[i]], a = c(quote(r), a))))
                ## Don't need any other from current environment. 
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
    
    ## Unlist if required.
    if(UNLIST) 
        return(unlist(res))
    
    res
}


## From http://www.cookbook-r.com/Graphs/Multiple_graphs_on_one_page_%28ggplot2%29/
multiplot <- function(..., plotlist=NULL, cols=1, layout=NULL) {
  require(grid)

  # Make a list from the ... arguments and plotlist
  plots <- c(list(...), plotlist)

  numPlots = length(plots)

  # If layout is NULL, then use 'cols' to determine layout
  if (is.null(layout)) {
    # Make the panel
    # ncol: Number of columns of plots
    # nrow: Number of rows needed, calculated from # of cols
    layout <- matrix(seq(1, cols * ceiling(numPlots/cols)),
                    ncol = cols, nrow = ceiling(numPlots/cols))
  }

 if (numPlots==1) {
    print(plots[[1]])

  } else {
    # Set up the page
    grid.newpage()
    pushViewport(viewport(layout = grid.layout(nrow(layout), ncol(layout))))

    # Make each plot, in the correct location
    for (i in 1:numPlots) {
      # Get the i,j matrix positions of the regions that contain this subplot
      matchidx <- as.data.frame(which(layout == i, arr.ind = TRUE))

      print(plots[[i]], vp = viewport(layout.pos.row = matchidx$row,
                                      layout.pos.col = matchidx$col))
    }
  }
}

