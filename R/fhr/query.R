##############################################################
### 
###  Code to extract and format FHR data from samples
### 
##############################################################

## Run query to extract FHR data from samples. 
## Optionally returns subsample. 
##
## Parameters: 
##
## output.folder - the HDFS folder to store output to. 
##    * Can be NULL, in which case data get stored in a temp folder 
##    * and can be read using z = query.fhr(...); rhread(z).
##
## data.in - a character vector giving the datasets to read from. 
##    * Can be one or more of "1pct", "5pct", "10pct", "nightly", "aurora", 
##      "beta", "prerelease", "full", or "fennec":
##    * where "prerelease" subsumes all three prerelease channels, 
##    * and at most one of the release samples "1pct","5pct","10pct" can be used.
##    * "fennec" and "full" should be used by themselves.
##    * Default is to use the 1% release sample only. 
##
## -- Query content --
## logic - the code to apply to valid FHR payloads meeting the conditions. 
## This can be used to extract the relevant fields from each payload and send 
## them to the reducer. 
##    * Should be a function taking a record key and an associated FHR payload.
##    *   eg. function(k,r) { res = ...; rhcollect(k,res) }
##    * Must include rhcollect statement for output to be emitted. 
##    * If unspecified, does nothing.
##    * If returns character string, occurrences of strings will be counted. 
##      This can be used to track the end state of the function. 
##      By default, will count # times function evaluates all the way to the 
##      last statement. 
##
## valid.filter - checks whether or not an FHR payload is valid. 
##    * Should be a function taking an FHR payload and evaluating to a boolean.
##    * Defaults to a sensible check function. 
##    * If explicitly NULL, filter matches all records. 
##
## conditions.filter - function to filter valid FHR payloads based on conditions
## (eg. date range). 
##    * The function will be applied to each FHR payload. If it does not return
##      TRUE, that payload will be ignored. The return value should generally
##      be a boolean, although it can also return a string message indicating
##      why the payload was not accepted. These strings will appear in the table
##      of end.state counts.
##    * If NULL, filter matches all records. 
##    * Default is is.standard.profile from FHR lib scripts.
##
## -- Sampling --
## prop - the approximate proportion of matching FHR payloads to retain. 
## num.out - the approximate number of matching FHR payloads to retain. 
##    * If neither is given, all records are retained. 
##    * If prop is given, each matching record is retained independently with 
##      probability=prop.
##    * If num.out is given but not prop, the proportion to keep is computed as
##      num.out/#records. 
##    * If both are given, prop takes precedence. 
##
## -- MR --
## input.folder - can specify the job input folder path directly. 
##    * This can be used to run jobs on custom datasets.
##    * To use this, data.in must be passed NULL explicitly. 
## reduce - the reducer to apply 
##    * Default is summer
## jobname - a name string to identify the job on the jobtracker page
##    * A timestamp will be appended. 
## attach.lib - if TRUE, the function will look for an environment named
##      ".fhrEnv" on the search path, and append the contents to the param
##      arg, if any. This makes the FHR lib functions available automatically.
##      Default is FALSE.
##
## ... - other arguments relating the the map-reduce job to be passed to rhwatch.
##    * These can include (among others):         
##          > setup - the setup code
##          > param - additional parameters to pass to RHIPE job
##          > 
##          > mapred - additional Hadoop MR parameters
##          > debug - debugging handler for RHIPE job (default is "count")
##          > read - whether or not to rhread the results (default is FALSE)
##
## Outputs the job handle z, augmented with a few additional values for 
## convenient access.
##    * z$input.data gives the path(s) of the input dataset used
##    * z$param is the final param list passed to the job
##    * z$n.used is the number of records that were passed to the logic function
##      after filtering
##    * z$n.output is the final number of records outputted
##    * z$stats is the matrix of filtering counters collected in the map phase
##    * z$end.state is a data table of end-state counters collected through the 
##        return value of the logic function
##    * z$mapred is the data table of relevant Map/Combine/Reduce record counts
##

fhr.query <- function(output.folder = NULL
                        ,data.in = "1pct"
                        ,logic = NULL
                        ,valid.filter = fhrfilter$v2()
                        ,conditions.filter = ff.cond.default()
                        ,prop = NULL
                        ,num.out = NULL
                        ,input.folder = NULL
                        ,reduce = summer
                        ,jobname = ""
                        ,attach.lib = FALSE
                        # ,setup = NULL
                        # ,param = list()
                        # ,mapred = NULL
                        # ,debug = "count"
                        ,...) {
    dots <- list(...)
    ## Resolve input directories to read data from. 
    ## First, the case where there is no named dataset (data.in).
    if(!is.character(data.in) || length(data.in) == 0 || 
                                                    all(!nzchar(data.in))) {
        ## Input path will be read from input.folder (must be supplied).
        if(!is.character(input.folder) || length(input.folder) == 0 || 
                                                    all(!nzchar(input.folder)))
            stop("Data source is not properly specified", call. = FALSE)
        ## Set data.in to empty to indicate we're not using samples. 
        data.in <- NULL
        input <- input.folder
    } else {
        ## Otherwise, input is given as named datasets using data.in.
        ## In this case, we shouldn't have input.folder at the same time.
        if(!is.null(input.folder)) {
            stop("Both data.in and input.folder have been specified. ",
                "Only one may be used.", call. = FALSE)
        }
        ## Now figure out what datasets to use based on data.in.
        data.in <- tolower(data.in)
        good.in <- data.in %in% c("1pct", "5pct", "10pct", "nightly", "aurora", 
                                    "beta", "prerelease", "full", "fennec")
        if(!all(good.in)) {
            stop(sprintf("Some data sources were not recognized: %s", 
                paste(data.in[!good.in], collapse = ", ")), call. = FALSE) 
        }
        
        ## Check for Fennec or full Desktop datasets. 
        if(any(c("full", "fennec") %in% data.in)) {
            ## Full datasets must be on their own.
            if(length(data.in) > 1) 
                stop("Full Desktop or Fennec must be specified on their own", 
                    call. = FALSE)
            ## Retrieve data path. 
            input <- fhrdir$fulldeorphaned(fhrversion = 
                                    if(identical(data.in, "fennec")) 3 else 2)
        } else {
            ## Enforce restrictions on combining input directories: 
            ## Read from at most 1 release sample.
            if(sum(c("1pct", "5pct", "10pct") %in% data.in) > 1) {
                message("*** Cannot combine multiple release samples. ", 
                    "Using 10pct sample")
                data.in <- data.in[!(data.in %in% c("1pct", "5pct"))]
            }
            ## If specifying "prerelease", read from all 3 prerelease channels. 
            if("prerelease" %in% data.in) {
                prch <- data.in %in% c("nightly", "aurora", "beta")
                if(any(prch)) {
                    message("*** Specifying 'prerelease' reads data from all ",
                        "3 prerelease channels. ",
                        "Ignoring individual prerelease channels")
                    data.in <- data.in[!prch]
                }
                ## Replace "prerelease" tag by individual channel tags.
                data.in <- data.in[data.in != "prerelease"]
                data.in <- append(data.in, c("nightly", "aurora", "beta"))
            }
            ## Expand to input paths.
            input <- fhrdir$sample(data.in)
        }
    }
    message(sprintf("Running job over %sdata located at:\n\t%s", 
        if(is.null(data.in)) "" else {
            sprintf("%s ", paste(data.in, collapse = ", "))
        },
        paste(input, collapse = ",\n\t")))
    
    ## Extract params arg - will be augmented with functions created below.
    param <- if(is.null(dots$param)) {
        message("No param argument supplied")
        list() 
    } else { 
        as.list(dots$param) 
    }
    dots$param <- NULL
    ## Add necessary basic functionality.
    if(is.null(param[["isn"]])) param[["isn"]] <- isn
    if(is.null(param[["get.val"]])) param[["get.val"]] <- get.val
    ## Add the FHR lib functions from the search path if required.
    if(isTRUE(attach.lib)) {
        fhrlib <- tryCatch(as.list(as.environment(".fhrEnv")),
            error = function(e) { NULL })
        if(length(fhrlib) > 0) {
            message("Attaching FHR lib functions to param list.")
            param <- c(param, fhrlib)
        } else {
            message("*** Attaching the FHR lib functions was requested, ",
                "but none were found. ")
        }
    }
    
    trivial.filter <- function(r) { TRUE }
    environment(trivial.filter) <- globalenv()
    
    ## Set the validity filter.
    param[["is.valid.packet"]] <- 
        if(missing(valid.filter)) {
            ## Missing arg means to use the default filter.
            ## Needs to be reset for fennec.
            if(identical(data.in, "fennec")) {
                message("Using default validity filter for v3")
                fhrfilter$v3()
            } else {
                message("Using default validity filter for v2")
                valid.filter
            }
        } else {
            ## An explicit NULL value means a trivial filter.
            if(is.null(valid.filter)) {
                message("No validity filter applied - all records accepted")
                trivial.filter
            } else {
                ## Otherwise we have a custom filter.
                ## Use it after applying some basic checks.
                if(!is.function(valid.filter))
                    stop("valid.filter is not a function")
                ## Check for uncalled fhrfilter. 
                if(identical(names(formals(valid.filter)), "count.fail")) {
                    stop("Function 'fhrfilter$v*' needs to be called",
                        "to create filter")
                }
                message("Using custom validity filter")
                valid.filter
            }
        }
        
    ## Set the conditions filter.
    param[["meets.conditions"]] <-
        if(missing(conditions.filter)) {
            ## Missing arg means to use the default filter.
            ## Needs to be reset for fennec.
            if(identical(data.in, "fennec")) {
                message("Using default conditions filter for v3")
                fennec.cond.default()
            } else {
                message("Using default conditions filter for v2")
                conditions.filter
            }
        } else {
            ## An explicit NULL value means a trivial filter.
            if(is.null(conditions.filter)) {
                message("No conditions filter applied - all records accepted")
                trivial.filter
            } else {
                ## Otherwise we have a custom filter.
                if(!is.function(conditions.filter))
                    stop("conditions.filter is not a function")
                message("Using custom conditions filter") 
                conditions.filter
            }
        }
   
    ## Apply subsetting if required. 
    ## If num.out is given, compute proportion to give target number 
    ## of output records. 
    ## If both prop and num.out are specified, prop wins. 
    if(is.na(isn(prop)) && !missing(num.out)) {
        num.out <- isn(as.numeric(num.out))
        if(is.na(num.out) || length(num.out) > 1 || num.out <= 0) {
            message("*** num.out is misspecified - ignoring. ",
                "If prop is supplied, that will be used ",
                "for subsetting instead.")
        }
        ## Cannot use num.out if not using samples. 
        if(is.null(data.in) || data.in %in% "fennec") {
            stop("Cannot use num.out if not using samples. ",
                "Specify prop instead to subsample", call. = FALSE)
        }
        ## Retrieve total number of records from job details for dataset. 
        n.records <- sum(sapply(input, function(nn) {
            rhload(sprintf("%s/_rh_meta/jobData.Rdata", nn))
            jobData[[1]]$counters[["Map-Reduce Framework"]][[
                "Reduce output records",1]]
        }))
        if(is.na(n.records) || n.records == 0) {
            stop("Unable to retrieve total number of records in dataset. ",
                "Cannot use num.out", call. = FALSE)
        }
        prop <- num.out / n.records
        message(sprintf("Sample size of %s records requested", num.out))
    }
    
    if(!missing(prop)) {
        prop <- isn(as.numeric(prop))
        if(is.na(prop) || length(prop) > 1 || prop <= 0 || prop >= 1) {
            message("*** prop is misspecified. No subsetting will be done")
            prop <- NULL
        }
    }
    param[["retain.current"]] <- 
        if(!is.null(prop)) {
            message(sprintf("Input data will be sampled at a rate of %s", 
                round(prop, 5)))
            pfun <- eval(substitute(function() { runif(1) < prop }, 
                list(prop=prop)))
            environment(pfun) <- globalenv()
            pfun
        } else {
            trivial.filter
        }
    
    ## Set the main logic function.
    param[["process.record"]] <-
        if(is.null(logic)) { 
            message("No logic function supplied. Map phase produces no output.")
            function(k, r) { } 
        } else { 
            if(!is.function(logic))
                stop("logic is not a function", call. = FALSE)
            ## Modify logic to return succesful end state upon completion. 
            fe <- body(logic)
            if(fe[[1]] == "{") {
                fe[[length(fe) + 1]] <- quote(return("COMPLETED"))
            } else {
                fe <- bquote({
                        .(fe)
                        return("COMPLETED")
                    }, list(fe = fe))
            }
            body(logic) <- fe
            logic
        }
        
    ## Mapper sanitizes records, applies filters, and applies query logic. 
    m <- function(k,r) {
        rhcounter("_STATS_", "NUM_PROCESSED", 1)
        ## Try parsing the JSON payload. 
        packet <- tryCatch({
            # r <- rawToChar(r[[1]])
            fromJSON(r)
        },  error = function(err) { NULL })
        if(is.null(packet)) {
            ## There was a problem parsing. 
            rhcounter("_STATS_", "BAD_JSON", 1)
            return()
        }
        rhcounter("_STATS_", "JSON_OK", 1)
        ## Check validity of data record. 
        ## NAs are treated as false. 
        valid <- is.valid.packet(packet)
        if(!(valid %in% TRUE)) {
            rhcounter("_STATS_", "INVALID_RECORD", 1)
            return()    
        }
        rhcounter("_STATS_", "VALID_RECORD", 1)
        ## Check if conditions are met. 
        cond <- meets.conditions(packet)
        if(!(cond %in% TRUE)) {
            rhcounter("_STATS_", "NOT_MEET_CONDITIONS", 1)
            if(is.character(cond)) {
                ## If filter function outputs strings (intended to indicate
                ## reason for exclusion, count occurrences.
                if(!grepl(":", cond)) cond <- sprintf("condfilter: %s", cond)
                rhcounter("_LOGIC_END_STATE_", cond, 1)
            }
            return()
        }
        rhcounter("_STATS_", "MEET_CONDITIONS", 1)
        ## Include current record at random. 
        if(!retain.current()) {
            rhcounter("_STATS_", "NUM_EXCLUDED", 1)
            return()
        }
        rhcounter("_STATS_", "NUM_RETAINED", 1)
        ## Finally, apply logic.
        ## Handle errors here (issue with error handling when rhwatch
        ## is called inside a function).
        end.state <- tryCatch(process.record(k, packet), 
            error = function(e) {
                e <- sprintf("%s: %s", deparse(e$call), e$message)
                rhcounter("R errors", e, 1)
                return("ERROR")
            })
        if(is.character(end.state))
            rhcounter("_LOGIC_END_STATE_", end.state, 1)
    }
    ## Don't enclose anything.
    environment(m) <- globalenv()
    
    ## Format job name. 
    if(!missing(jobname)) {
        jobname <- tryCatch(isn(as.character(jobname)), 
            error = function(e) { "" })
        jobname <- jobname[[1]]
    }
    ## Add timestamp. 
    tstamp <- format(Sys.time() - as.difftime(6, units = "hours"))
    jobname <- if(!is.na(jobname) && nzchar(jobname)) {
        jobname <- sprintf("%s | %s", jobname, tstamp)
    } else {
        tstamp
    }
    
    ## Add setup code - need to load rjson package. 
    setup <- if(is.null(dots$setup)) {
            expression(map = { library(rjson) })
        } else {
            ## Already have setup expression. 
            ## Add to "map" element. 
            setup <- as.list(dots$setup)
            setup[["map"]] <- if(is.null(setup$map))
                    quote({ library(rjson) })
                else
                    bquote({
                        library(rjson)
                        .(prev)
                    }, list(prev = setup$map))
            as.expression(setup)
        }
    dots$setup <- NULL
    
    ## Default value of debug should be "count". 
    debug.val <- if(is.null(dots$debug)) "count" else dots$debug
    dots$debug <- NULL
    
    ## Default vaule of read should be FALSE. 
    read.val <- if(is.null(dots$read)) FALSE else dots$read
    dots$read <- NULL
        
    ## Run job. 
    message("Launching job...")
    z <- do.call(rhwatch, c(list(
            map = m 
            ,reduce = reduce
            ,input = sqtxt(input)
            # ,input = hbasefmt(table="metrics" ,colspec="data:json")
            ,output = output.folder
            ## Wrap referenced objects into parameter functions
            # ,param = wrap.fun(param)
            ,param = param
            ,setup = setup
            ,jobname = jobname
            ,debug = debug.val
            ,read = read.val
        ), dots)
    )
    
    ## Inform on job completion, unless noeval is used for testing.
    if(!isTRUE(dots$noeval)) {
        if(z[[1]]$rerrors || !identical(z[[1]]$state, "SUCCEEDED")) {
            message("*** Job did not complete sucessfully.")
        } else {
            message("Job completed. Formatting counters...")
        }
    }
    
    ## Record input datasets passed.  
    z[["input.data"]] <- input
    
    ## Format counter information
    counters <- list(
    ## Shortcut to relevant Map-Reduce counters. 
    try({
        mrf <- z[[1]][[c("counters", "Map-Reduce Framework")]]
        if(!is.null(mrf)) {
            mrf <- data.table(counter = rownames(mrf), n = mrf[,1])[
                ## Keep Map, Combine, and Reduce I/O counters. 
                grepl("^Map|Combine|Reduce", counter)][
                order(counter)]
            mrf <- mrf[unlist(lapply(c("Map", "Combine", "Reduce"), 
                function(cn) { grep(sprintf("^%s", cn), counter) }))]
            z[["mapred"]] <- mrf
            if("Reduce output records" %in% mrf$counter)
                z[["n.output"]] <- mrf[counter == "Reduce output records"][, n]
        }
        NULL
    }, silent = TRUE),
    
    ## Init stats counters. 
    try({
        stats <- z[[1]][[c("counters","_STATS_")]]
        if(!is.null(stats)) {
            stats <- data.table(counter = rownames(stats), n = stats[,1],
                key = "counter") 
            ## Add succesive percentages.
            stats[counter != "NUM_PROCESSED", 
                pct := round(n / stats["NUM_PROCESSED"][, n] * 100, 3)]
            stats[c("MEET_CONDITIONS", "NUM_RETAINED", "NUM_EXCLUDED", 
                    "NOT_MEET_CONDITIONS"),
                pct.valid := round(n / stats["VALID_RECORD"][, n] * 100, 3)]
            stats[c("NUM_RETAINED", "NUM_EXCLUDED"),
                pct.cond := round(n / stats["MEET_CONDITIONS"][, n] * 100, 3)]
            stats[is.na(stats)] <- ""
            ## Reorder counters in hierarchical order (from alphabetical). 
            stats <- stats[c(
                "NUM_PROCESSED", 
                "JSON_OK", 
                "VALID_RECORD", 
                "MEET_CONDITIONS", 
                "NUM_RETAINED", 
                "NUM_EXCLUDED",
                "NOT_MEET_CONDITIONS", 
                "INVALID_RECORD", 
                "BAD_JSON"), 
                nomatch = 0]
            z[["stats"]] <- stats
            z[["n.used"]] <- if("NUM_RETAINED" %in% stats$counter) {
                stats[counter == "NUM_RETAINED"][, n] 
            } else 0
        }
        NULL
    }, silent = TRUE),
    
    ## Logic end states.
    try({
        es <- z[[1]][[c("counters", "_LOGIC_END_STATE_")]]
        if(!is.null(es)) {
            es <- setnames(data.table(rownames(es), es), c("condition", "count"))
            es <- es[, { 
                cond.str <- strsplit(condition, ": ", fixed = TRUE)
                list(stage = sapply(cond.str, function(s) { 
                    if(length(s) == 2) s[1] else "" }),
                condition = sapply(cond.str, function(s) { s[length(s)] }),
                count = count) 
            }][order(stage, condition)]
            ## Remove stage column if completely empty.
            if(es[, all(!nzchar(stage))]) es[, stage := NULL]
            ## Add percentages for counts.
            es[, pct := to.pct(count)]
            ## Add percentages for counts that are part of the main job
            ## and not related to filtering.
            filterstages <- c("init", "condfilter")
            if(any(filterstages %in% unique(es$stage)))
                es[!(stage %in% filterstages), pct.main := to.pct(count)]
            z[["end.state"]] <- es
        }
        NULL
    }, silent = TRUE)
    )
    if("try-error" %in% unlist(lapply(counters, class)))
        message("*** There was an error processing counter information.")
    
    message("Done!")
    z
}

