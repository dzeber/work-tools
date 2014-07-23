#######################################################################
###  
###  Shortcuts for accessing FHR stored on HDFS.
###  
###  Creates list 'fhr.dir' containing functions 'sample' and 
###  'fennec', which return the appropriate HDFS path. 
###  Function 'sample' takes sample name as argument.
###  
###  Also function to load some recent FHR packets to work with. 
###  
#######################################################################


fhrdir <- list()

## Expand FHR samples names to full path. 
## Input should be a subset of "1pct", "5pct", "nightly", "aurora", "beta", 
## but this is not checked. 

fhrdir$sample <- function(samp = c("1pct", "5pct", "nightly", "aurora", "beta","fromjson1pct")) {
    samp <- match.arg(samp, several.ok = TRUE)
    sapply(samp, function(nn) { 
        sprintf("/user/sguha/fhr/samples/output/%s", nn)
    }, USE.NAMES = FALSE)
}


## Expand path to recent dump of full FHR data. 
## Uses second most recent day. 
## Input is version (2 or 3), and optionally a subfolder/dataset name 
## (which is actually a date). 

# fhr.full.dir = function(v = 2, dataset = NULL) {
    # if(!(v %in% c(2,3)))
        # stop("version must be either 2 or 3")
    # data.dir = "/data/fhr/nopartitions/"
    # if(is.null(dataset)) {
        # dataset = grep("\\d+$", rhls(data.dir)$file, value = TRUE)
        # dataset = sort(sub(data.dir, "", dataset))
        # dataset = dataset[length(dataset) - 1]
    # }
    # sprintf("%s%s/%s", data.dir, dataset, v)
# }


## Get HDFS path to recent Fennec data. 
## Use date which is at least 2 days older than today. 
## If not available, return earliest date with a warning.

fhrdir$fennec <- function() {
    data.dir = "/data/fhr/nopartitions"
    ## Data directories are named by date. 
    days = grep("\\d{8}$", rhls(data.dir)$file, value = TRUE)
    if(length(days) == 0) 
        stop("No data dir found")
    
    days = sort(basename(days), decreasing = TRUE)
    ## Check for dates that are at least two days old. 
    old.enough = as.numeric(Sys.Date() - as.Date(days, "%Y%m%d")) >= 2
    
    data.day = if(!any(old.enough)) {
        ## If all dates are more recent than 2 days, use oldest with a warning.
        warning("All available dates are less than two days old - using oldest")
        days[length(days)]
    } else {
        ## Otherwise, use most recent date that is at least 2 days old. 
        days[which(old.enough)[1]]
    }
    
    file.path(data.dir, data.day, 3)
}



## Load a few FHR records to work with. 
## Specify the number to load (load same number from each source). 
## Returns a list of FHR records stripped of keys,
## ie. x = fhr.load.some() 
## and then we can do x[[1]]$data instead of x[[1]][[2]]$data.

fhr.load.some = function(n.records = 100, samp = "1pct") {
    if(!is.numeric(n.records) && n.records <= 0)
        stop("n.records is invalid")
        
    if(length(samp) > 1)
        stop("Only one source should be specified")

    require("rjson")
    
    data.dir = if(identical(samp, "fennec")) {
        ## For Fennec, need to refer to part files specifically. 
        f = rhls(fhrdir$fennec())$file
        f[grepl("^part-", basename(f))]
    } else {
        fhrdir$sample(samp)
    }

    ## fromjson1pct has a different handling scheme
    if(grepl("fromjson",data.dir)){
        isTextual <- FALSE
    }else{
        isTextual <- TRUE
    }

    ## Load records.
    r <- rhread(data.dir, max = n.records, textual = isTextual)
    
    ## Convert to R lists. 
    r = if(isTextual){
        lapply(r, function(s) {
            tryCatch({ deserialize(s[[2]]) },  error=function(e) { NULL })
        })
    } else {
        lapply(r, "[[",2)
    }
    
    r.null = sapply(r, is.null)
    if(any(r.null)) warning("Some records could not be parsed.")
    r[!r.null]
}

