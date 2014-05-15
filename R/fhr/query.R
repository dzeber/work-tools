##############################################################
### 
###  Code to extract and format FHR data from samples
### 
##############################################################


#####################################################################
#####
#####  NB: TO USE THIS FILE ON HALA, DO NOT SOURCE DIRECTLY.
#####  
#####  INSTEAD, USE source("/usr/local/share/load-fhr-tools.R")
#####
#####################################################################



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
##    * Can be one or more of "1pct", "5pct", "nightly", "aurora", "beta", "prerelease", "fennec"
##    * where "prerelease" subsumes all three prerelease channels, 
##    * and at most one of the release samples "1pct" and "5pct" can be used.
##    * "fennec" should also be used by itself.
##    * Default is to use the 1% release sample only. 
##
## -- Query content --
## logic - the code to apply to valid FHR packets meeting the conditions. 
## This can be used to extract the relevant fields from each payload and send them to the reducer. 
##    * Should be a function taking a record key and an associated FHR packet.
##    *   eg. function(k,r) { res = ...; rhcollect(k,res) }
##    * Must include rhcollect statement for output to be emitted. 
##    * If unspecified, does nothing.
##    * If returns character string, occurrences of strings will be counted. 
##      This can be used to track the end state of the function. 
##      By default, will count # times function evaluates all the way to the last statement. 
##
## valid.filter - checks whether or not an FHR packet is valid. 
##    * Should be a function taking an FHR packet and evaluating to a boolean.
##    * Defaults to a sensible check function. 
##    * If explicitly NULL, filter matches all records. 
##
## conditions.filter - function to filter valid FHR packets based on conditions (eg. date range). 
##    * Should be a function taking an FHR packet and evaluating to a boolean. ##    * If NULL, filter matches all records. 
##    * Default is vendor="Mozilla", name="Firefox" (or "fennec" for Fennec data).
##
## -- Sampling --
## prop - the approximate proportion of matching FHR packets to retain. 
## num.out - the approximate number of matching FHR packets to retain. 
##    * If neither is given, all records are retained. 
##    * If prop is given, each matching record is retained independently with probability=prop.
##    * If num.out is given but not prop, the proportion to keep is computed as num.out/#records. 
##    * If both are given, prop takes precedence. 
##
## -- MR --
## input.folder - can specify the job input folder path directly. 
##    * This can be used to run jobs on the full v2 or v3 FHR data. 
##    * To use this, data.in must be passed NULL explicitly. 
## ... - other arguments relating the the map-reduce job to be passed to rhwatch.
##    * These can include (among others):         
##          > reduce - the reducer to apply 
##          > setup - the setup code
##          > param - additional parameters to pass to RHIPE job
##          > jobname - a name string to identify the job on the jobtracker page
##              * If specified, a timestamp will be appended. 
##          > mapred - additional Hadoop MR parameters
##          > debug - debugging handler for RHIPE job (default is "count")
##          > read - whether or not to rhread the results (default is FALSE)
##
## Outputs the job handle z, augmented with a few additional values for convenient access.
##    * z$input.data is a string representing the input dataset
##    * z$param is the final param list passed to the job

fhr.query = function(output.folder = NULL
                    ,data.in = "1pct"
                    ,logic = NULL
                    ,valid.filter = fhrfilter$v2()
                    ,conditions.filter = ff.cond.default()
                    ,prop = NULL
                    ,num.out = NULL
                    ,input.folder = NULL
                    # ,reduce = NULL
                    # ,setup = NULL
                    # ,param = list()
                    # ,jobname = ""
                    # ,mapred = NULL
                    # ,debug = "count"
                    ,...
                ) {
    dots = list(...)
    
    ## Resolve input directories to read data from. 
    if(!is.character(data.in) || length(data.in) == 0 || data.in == "") {
        if(!is.character(input.folder) || length(input.folder) == 0 || input.folder == "")
            stop("Data source is not properly specified")
        
        ## data.in not given but input.folder is.
        ## Set data.in to NULL to indicate we're not using samples. 
        data.in = NULL
        input = input.folder
    } else {
        ## Both are given. 
        if(!is.null(input.folder))
            stop("Both data.in and input.folder have been specified. Only one may be used.")
        
        ## Otherwise use data.in
        data.in = tolower(data.in)
        good.in = data.in %in% c("1pct", "5pct", "nightly", "aurora", "beta", "prerelease", "fennec")
        if(!all(good.in))
            stop(sprintf("Some data sources were not recognized: %s", paste(data.in[!good.in], collapse = ", "))) 
            
        ## Check for Fennec or desktop. 
        if("fennec" %in% data.in) {
            ## Fennec must be on its own.
            if(length(data.in) > 1)
                stop("Fennec must be specified on its own")
            
            ## Retrieve data path. 
            input = fhrdir$fennec()
        } else {
            ## Enforce restrictions on combining input directories: 
            ## Read from at most 1 release sample.
            if(all(c("1pct", "5pct") %in% data.in)) {
                warning("Cannot combine both release samples. Using 5pct sample")
                data.in = data.in[data.in != "1pct"]
            }
            ## If specifying "prerelease", read from all 3 prerelease channels. 
            if("prerelease" %in% data.in) {
                prch = data.in %in% c("nightly", "aurora", "beta")
                if(sum(prch) > 0) {
                    warning("Specifying 'prerelease' reads data from all 3 prerelease channels. Ignoring individual prerelease channels")
                    data.in = data.in[!prch]
                }
                ## Replace "prerelease" tag by individual channel tags.
                data.in = data.in[data.in != "prerelease"]
                data.in = c(data.in, c("nightly", "aurora", "beta"))
            }
            ## Expand to input paths.
            input = fhrdir$sample(data.in)
        }
    } 
    
    ## If data.in is set to "fennec" and filters are missing, set to default v3 filters. 
    if(identical(data.in, "fennec")) {
        if(missing(valid.filter)) {
            valid.filter = fhrfilter$v3()
        }
        if(missing(conditions.filter)) {
            conditions.filter = fennec.cond.default()
        }
    }
    
    param = if(is.null(dots$param)) list() else as.list(dots$param)
    dots$param = NULL
    
    ## Should be included as necessary by wrap.fun(param) in rhwatch call.
    ## (although not working yet)
    if(is.null(param[["isn"]]))
        param[["isn"]] = isn
    if(is.null(param[["get.val"]]))
        param[["get.val"]] = get.val
    
    param[["is.valid.packet"]] = 
        if(is.null(valid.filter)) { 
            function(r) { TRUE } 
        } else { 
            if(!is.function(valid.filter))
                stop("valid.filter is not a function")
            ## Add check for uncalled fhrfilter. 
            if(identical(names(formals(valid.filter)), "count.fail"))
                stop("Function 'fhrfilter$v*' needs to be called to create filter")
            # wrap.fun(
            valid.filter
            # )
        }    
 
    param[["meets.conditions"]] = 
        if(is.null(conditions.filter)) { 
            function(r) { TRUE } 
        } else { 
            if(!is.function(conditions.filter))
                stop("conditions.filter is not a function")
            # wrap.fun(
            conditions.filter 
            # )
        }
        
    ## If num.out is given, compute proportion to give target number of output records. 
    ## If both prop and num.out are specified, prop wins. 
    if(is.numeric(num.out) && !is.na(num.out) && length(num.out) == 1 && num.out > 0 
            && is.na(isn(prop))) {
        ## Cannot use num.out if not using samples. 
        if(is.null(data.in))
            stop("Cannot use num.out if not using samples. Specify prop instead to subsample")
        
        ## Retrieve total number of records from job details for dataset. 
        n.records = sum(sapply(input, function(nn) {
            rhload(sprintf("%s/_rh_meta/jobData.Rdata", nn))
            jobData[[1]]$counters[["Map-Reduce Framework"]][["Reduce output records",1]]
        }))
        if(is.na(isn(n.records)))
            stop("Unable to retrieve total number of records in dataset. Cannot use num.out")
        prop = num.out / n.records
    }
    
    param[["retain.current"]] = 
        if(is.numeric(prop) && !is.na(prop) && length(prop) == 1 && prop > 0 && prop < 1) { 
            eval(substitute(function() { runif(1) < prop }, list(prop=prop)))
        } else {
            function() { TRUE }
        }
        
    param[["process.record"]] = 
        if(is.null(logic)) { function(k, r) { } } else { 
            if(!is.function(logic))
                stop("logic is not a function")
                
            ## Modify logic to return succesful end state upon completion. 
            fe = body(logic)
            if(fe[[1]] == "{") {
                fe[[length(fe) + 1]] = quote(return("COMPLETED"))
            } else {
                fe = bquote({
                        .(fe)
                        return("COMPLETED")
                    }, list(fe = fe))
            }
            body(logic) = fe
            logic
        }
        
    ## Check param contents
    # for(nn in names(param)) {
        # print(sprintf("%s: ", nn))
        # print(ls(environment(param[[nn]])))
    # }
    
    ## Mapper sanitizes records, applies filters, and applies query logic. 
    m = function(k,r) { try({
        rhcounter("_STATS_", "NUM_PROCESSED", 1)
        
        ## Try parsing the JSON payload. 
        packet = tryCatch({
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
        valid = is.valid.packet(packet)
        if(!ifelse(is.na(valid), FALSE, valid)) {
            rhcounter("_STATS_", "INVALID_RECORD", 1)
            return()    
        }
        
        rhcounter("_STATS_", "VALID_RECORD", 1)
            
        ## Check if conditions are met. 
        cond = meets.conditions(packet)
        if(!ifelse(is.na(cond), FALSE, cond)) {
            rhcounter("_STATS_", "NOT_MEET_CONDITIONS", 1)
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
        end.state = process.record(k, packet)
        if(is.character(end.state))
            rhcounter("_LOGIC_END_STATE_", end.state, 1)
    })}
    
    ## Format job name. 
    jobname = if(is.null(dots$jobname)) "" else dots$jobname
    dots$jobname = NULL
    
    if(!is.character(jobname))
        jobname = tryCatch(as.character(jobname), error = function(e) { "" })
    jobname = jobname[[1]]
    ## Add timestamp. 
    jobname = ifelse(nchar(jobname) > 0, sprintf("%s | %s", jobname, Sys.time()), "")
    
    ## Add setup code - need to load rjson package. 
    setup =  if(is.null(dots$setup)) {
            expression(map = { library(rjson) })
        } else {
            ## Already have setup expression. 
            ## Add to "map" element. 
            setup = as.list(dots$setup)
            setup[["map"]] = if(is.null(setup$map))
                    quote({ library(rjson) })
                else
                    bquote({
                        library(rjson)
                        .(prev)
                    }, list(prev = setup$map))
            as.expression(setup)
        }
    dots$setup = NULL
    
    ## Default value of debug should be "count". 
    debug.val = if(is.null(dots$debug)) "count" else dots$debug
    dots$debug = NULL
    
    ## Default vaule of read should be FALSE. 
    read.val = if(is.null(dots$read)) FALSE else dots$read
    dots$read = NULL
        
    ## Run job. 
    z = do.call(rhwatch, c(list(
        # rhwatch(
                map = m 
                #,reduce = reduce
                ,input = sqtxt(input)
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
                      
    if(z[[1]]$rerrors || z[[1]]$state=="FAILED") {
        warning("Job did not complete sucessfully.")
        # return(z)
    }
    
    ## Record input datasets and parameters passed.  
    z[["input.data"]] = ifelse(is.null(data.in), input, data.in)
    z[["param"]] = param
    
    ## Shortcut relevant Map-Reduce counters. 
    mrf = z[[1]][[c("counters", "Map-Reduce Framework")]]
    ## Keep Map, Combine, and Reduce I/O counters. 
    mrf.rows = grep("^Combine", rownames(mrf))
    mrf.rows = c(grep("^Map", rownames(mrf)), 
        # if(any(mrf[mrf.rows,1] > 0)) mrf.rows else NULL, 
        mrf.rows,
        grep("^Reduce", rownames(mrf)))
    z[["mapred"]] = as.matrix(mrf[mrf.rows,])
    
    ## Format counters, if available. 
    zz = tryCatch({
        stats = z[[1]][[c("counters","_STATS_")]]
        if(!is.null(stats)) {
            ## Reorder counters in hierarchical order (from alphabetical). 
            ctrs = c("NUM_PROCESSED", "JSON_OK", "VALID_RECORD", "MEET_CONDITIONS",
                "NUM_RETAINED", "NUM_EXCLUDED", "NOT_MEET_CONDITIONS", "INVALID_RECORD", "BAD_JSON")
            ctrs = ctrs[ctrs %in% rownames(stats)]
            stats = as.matrix(stats[ctrs,])
            z[["stats"]] = stats
            z[["count"]] = ifelse(!is.na(stats[["NUM_RETAINED",1]]), stats[["NUM_RETAINED",1]], 0)
        }
        es = z[[1]][[c("counters", "_LOGIC_END_STATE_")]]
        if(!is.null(es))
            z[["end.state"]] = es
        z
    }, error = function(err) { NULL })
    if(!is.null(zz))
        z = zz
    z
}

##    * z$count is the number of records that were passed to the logic function after filtering
##    * z$stats is the matrix of filtering counters collected in the map phase
##    * z$end.state is the matrix of end-state counters collected through the ##        return value of the logic function
##    * z$mapred is the matrix of relevant Map/Combine/Reduce record counts
##