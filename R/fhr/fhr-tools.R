##############################################################
### 
###  Utility R code for working with FHR
### 
##############################################################


## Robust accessor for FHR values. 
## Retrieves the element with name n from data list/vector d.
## Returns NA if no such element. 

get.val = function(d, n) { isn(d[n][[1]]) }


## Expand FHR samples names to full path. 
## Input should be a subset of "1pct", "5pct", "nightly", "aurora", "beta", 
## but this is not checked. 

fhr.sample.dir = function(samp = c("1pct", "5pct", "nightly", "aurora", "beta")) {
    samp = match.arg(samp, several.ok = TRUE)
    sapply(samp, function(nn) { 
        sprintf("/user/sguha/fhr/samples/output/%s", nn)
    }, USE.NAMES = FALSE)
}


## Expand path to recent dump of full FHR data. 
## Uses second most recent day. 
## Input is version (2 or 3), and optionally a subfolder/dataset name 
## (which is actually a date). 

fhr.full.dir = function(v = 2, dataset = NULL) {
    if(!(v %in% c(2,3)))
        stop("version must be either 2 or 3")
    data.dir = "/data/fhr/nopartitions/"
    if(is.null(dataset)) {
        dataset = grep("\\d+$", rhls(data.dir)$file, value = TRUE)
        dataset = sort(sub(data.dir, "", dataset))
        dataset = dataset[length(dataset) - 1]
    }
    sprintf("%s%s/%s", data.dir, dataset, v)
}


## Load a few FHR records to work with, from one or more samples. 
## Specify the number to load (load same number from each source). 
## Returns a list of FHR records stripped of keys,
## ie. x = fhr.load.some() 
## and then we can do x[[1]]$data instead of x[[1]][[2]]$data.

fhr.load.some = function(n.records = 100, samp = "1pct") {
    if(!is.numeric(n.records) && n.records <= 0)
        stop("n.records is invalid")
    
    require("rjson")
    r = do.call(c, lapply(fhr.sample.dir(samp), rhread, max = n.records, textual = TRUE))
    r = lapply(r, function(s) {
        tryCatch({ fromJSON(s[[2]]) },  error=function(e) { NULL })
    })
    r.null = sapply(r, is.null)
    if(any(r.null)) warning("Some records could not be parsed.")
    r[!r.null]
}




