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

