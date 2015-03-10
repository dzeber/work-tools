##############################################################
### 
###  R code for accessing FHR payload fields.
### 
##############################################################


## Robust accessor for FHR values. 
## Retrieves the element with name n from data list/vector d.
## Returns NA if no such element. 

get.val = function(d, n) { isn(d[n][[1]]) }



## Check validity of strings intended to represent dates using regex. 
## In FHR, the format should be yyyy-mm-dd.
## Returns TRUE if all elements of input are correctly formatted, FALSE otherwise.

valid.dates = function(d) {
    ## Use regex to check for valid date format between 1900-01-01 and 2999-12-31.
    all(grepl("^(19|2[0-9])\\d{2}-((0[13578]|1[02])-(0[1-9]|[12][0-9]|3[01])|(0[469]|11)-(0[1-9]|[12][0-9]|30)|02-(0[1-9]|[12][0-9]))$", d))
}



