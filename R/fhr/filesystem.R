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
## Input should be a subset of "1pct", "5pct", "10pct", "nightly", "aurora", 
## "beta", or "fromjson1pct", but this is not checked. 

fhrdir$sample <- function(samp = c("1pct", "5pct", "10pct", "nightly", 
                                            "aurora", "beta","fromjson1pct")) {
    samp <- match.arg(samp, several.ok = TRUE)
    sprintf("/user/sguha/fhr/samples/output/%s", samp)
}

## Get HDFS path to recent full deorphaned Desktop data. 

fhrdir$fulldeorphaned <- function(fhrversion = 2) {
    if(!(fhrversion %in% 2:3)) stop("fhrversion must be either 2 or 3")
    data.dir <- "/user/bcolloran/deorphaned"
    ## Data directories are named by date. 
    days <- grep("\\d{4}-\\d{2}-\\d{2}$", rhls(data.dir)$file, value = TRUE)
    if(length(days) == 0) stop("No data dir found")
    days <- days[order(basename(days), decreasing = TRUE)]
    ## Return the path to the most recent dataset.
    file.path(days[[1]], sprintf("v%s", fhrversion))
}


## Get HDFS path to recent raw Fennec data. 
## Use date which is at least 2 days older than today. 
## If not available, return earliest date with a warning.

fhrdir$fennec <- function() {
    data.dir <- "/data/fhr/nopartitions"
    ## Data directories are named by date. 
    days <- grep("\\d{8}$", rhls(data.dir)$file, value = TRUE)
    if(length(days) == 0) 
        stop("No data dir found")
    
    days <- sort(basename(days), decreasing = TRUE)
    ## Check for dates that are at least two days old. 
    old.enough <- as.numeric(Sys.Date() - as.Date(days, "%Y%m%d")) >= 2
    
    data.day <- if(!any(old.enough)) {
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

fhr.load.some <- function(n.records = 100, samp = "1pct") {
    if(!is.numeric(n.records) && n.records <= 0)
        stop("n.records is invalid")
        
    if(length(samp) > 1)
        stop("Only one source should be specified")

    require("rjson")
    data.dir <- if(identical(samp, "fennec")) {
        ## For Fennec, need to refer to part files specifically. 
        f <- rhls(fhrdir$fennec())$file
        f[grepl("^part-", basename(f))]
    } else {
        fhrdir$sample(samp)
    }

    ## fromjson1pct has a different handling scheme
    isTextual <- !any(grepl("fromjson",data.dir))
    
    ## Load records.
    r <- rhread(data.dir, max = n.records, textual = isTextual)
    
    ## Convert to R lists. 
    r = if(isTextual){
        lapply(r, function(s) {
            tryCatch({ fromJSON(s[[2]]) },  error=function(e) { NULL })
        })
    } else {
        lapply(r, "[[", 2)
    }
    
    r.null <- sapply(r, is.null)
    if(any(r.null)) warning("Some records could not be parsed.")
    r[!r.null]
}

## Effective dates from current snapshot. 
## These are dates for which all available FHR data should be represented 
## (ie. not exceeding the 180-day window, and 2 weeks earlier than 
## the snapshot date).
## If months = TRUE, date bounds are rounded to calendar months 
## contained in date range.
current.snapshot.dates <- function(month = FALSE) {
    ## Read current snapshot date from deorphanded data dir.
    curr.date <- max(basename(rhls("/user/bcolloran/deorphaned/")$file))
    curr.date <- as.Date(curr.date)
    
    earliest <- curr.date - 180
    latest <- curr.date - 15
    if(month) {
        ## Round earliest up to next month.
        earliest <- earliest - as.POSIXlt(earliest)$mday + 1
        earliest <- seq(earliest, by = "month", length.out = 2)[2]
        ## Round latest down to last day of previous month. 
        latest <- latest - as.POSIXlt(latest)$mday
    }
    list(earliest = earliest, latest = latest)
}


## Create table of available FHR snapshots together with effective dates. 
## Dates are considered in monthly chunks.
fhr.snapshots.mth <- function() {
    snapshot.dir <- "/user/sguha/fhr/samples/backup"
    snapshots <- data.table(snapshot = basename(rhls(snapshot.dir)$file))
    
    snapshots[, earliest := {
        d <- as.Date(snapshot) - 180
        ## Round up to the next month.
        d <- d - as.POSIXlt(d)$mday + 1
        d <- do.call(c, lapply(d, function(dd) {
            seq(dd, by = "month", length.out = 2)[2]
        }))
        as.character(d)
    }]  
    snapshots <- snapshots[, list(snapshot = max(snapshot)), by = earliest]
    
    snapshots[, latest := {
        curr <- as.Date(max(snapshot)) - 15
        curr <- curr - as.POSIXlt(curr)$mday + 1
        c(earliest[-1], as.character(curr))
    }]
    
    snapshots[, snapshot := file.path(snapshot.dir, snapshot)]
    setcolorder(snapshots, c("snapshot", "earliest", "latest"))
    snapshots
}



