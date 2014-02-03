##############################################################
### 
###  Utility R code for working with FHR
### 
##############################################################


## Robust accessor for FHR values. 
## Retrieves the element with name n from data list/vector d.
## Returns NA if no such element. 

get.val = function(d, n) { isn(d[n][[1]]) }



