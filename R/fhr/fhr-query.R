##############################################################
### 
###  Code to extract and format FHR data from samples
### 
##############################################################


## Need to source rhipe-tools first for wrap.fun(), and fhr-tools

##### NB: NEED TO SOURCE THE CURRENT FILE USING chdir = TRUE: 
#####    source(".../R/fhr/fhr-query.R", chdir = TRUE)
##### FOR THE FOLLOWING TO WORK

source("./fhr-tools.R")
if(!exists("wrap.fun"))
    source("../rhipe/rhipe-tools.R")


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
##    * Can be one or more of "1pct", "5pct", "nightly", "aurora", "beta", "prerelease", 
##    * where "prerelease" subsumes all three prerelease channels, 
##    * and at most one of the release samples "1pct" and "5pct" can be used.
##    * Default is to use the 1% release sample only. 
##
## -- Query content --
## logic - the code to apply to valid FHR packets meeting the conditions. 
## This can be used to extract the relevant fields from each payload and send them to the reducer. 
##    * Should be a function taking a record key and an associated FHR packet.
##    *   eg. function(k,r) { res = ...; rhcollect(k,res) }
##    * Must include rhcollect statement for output to be emitted. 
##    * If unspecified, does nothing.
##
## valid.filter - checks whether or not an FHR packet is valid. 
##    * Should be a function taking an FHR packet and evaluating to a boolean.
##    * Defaults to a sensible check function. 
##    * If explicitly NULL, filter matches all records. 
##
## conditions.filter - function to filter valid FHR packets based on conditions (eg. date range). 
##    * Should be a function taking an FHR packet and evaluating to a boolean. ##    * If NULL, filter matches all records. 
##    * Default is vendor="Mozilla", name="Firefox" on standard channels and OSs. 
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
## reduce - the reducer to apply 
## param - additional parameters to pass to RHIPE job
## debug - debugging handler for RHIPE job 
##
## Outputs the job handle z. 
## Counters can be accessed using z$stats and the total number of outputted records is accessible as z$count. 

fhr.query = function(output.folder = NULL
                    ,data.in = "1pct"
                    ,logic = NULL
                    ,valid.filter = v2.filter.gen()
                    ,conditions.filter = cond.default()
                    ,prop = NULL
                    ,num.out = NULL
                    ,input.folder = NULL
                    ,reduce = NULL
                    ,param = list()
                    ,mapred = NULL
                    ,debug = "count"
                ) {
    
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
        good.in = data.in %in% c("1pct", "5pct", "nightly", "aurora", "beta", "prerelease")
        if(!all(good.in))
            stop(sprintf("Some data sources were not recognized: %s", paste(data.in[!good.in], collapse = ", "))) 
            
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
        input = fhr.sample.dir(data.in)
    } 
    
    ## Should be included as necessary by wrap.fun(param) in rhwatch call.
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
            logic 
        }
        
    ## Check param contents
    # for(nn in names(param)) {
        # print(sprintf("%s: ", nn))
        # print(ls(environment(param[[nn]])))
    # }
    
    ## Mapper sanitizes records, applies filters, and applies query logic. 
    m = function(k,r) {
        rhcounter("_STATS_", "NUM_PROCESSED", 1)
        
        ## Try parsing the JSON payload. 
        packet = tryCatch({
            fromJSON(r)
        },  error=function(err) { NULL })
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
        process.record(k, packet)
    }
    
    ## Run job. 
    z = rhwatch(map = m 
                ,reduce = reduce
                ,input = sqtxt(input)
                ,output = output.folder
                ## Wrap referenced objects into parameter functions
                # ,param = wrap.fun(param)
                ,param = param
                ,setup = expression(map = { library(rjson) })
                ,mapred = mapred
                ,debug = debug
                ,read = FALSE
    )
                      
    if(z[[1]]$rerrors || z[[1]]$state=="FAILED") {
        warning("Job did not complete sucessfully.")
        return(z)
    }
    
    ## Record input datasets. 
    z[["input.data"]] = ifelse(is.null(data.in), input, data.in)
    
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
        z
    }, error=function(err) { NULL })
    if(!is.null(zz))
        z = zz
    z
}


## Validity filter for v2 FHR. 

v2.filter.gen = function(count.fail = FALSE) {
    conds = list(
        ## version 2 FHR
        fhr.version = quote(!is.null(r$version) && r$version == 2),
        ## Has geckoAppInfo - will be used to read app info
        geckoAppInfo = quote(is.character(r$geckoAppInfo) || is.list(r$geckoAppInfo)),
        ## Has data field
        data = quote(is.list(r$data)),
        ## Has info from last session
        last = quote(is.list(r$data$last)),
        ## Has days field
        days = quote(is.list(r$data$days)),
        ## Has system info
        sysinfo = quote(is.character(r$data$last$org.mozilla.sysinfo.sysinfo) || 
            is.list(r$data$last$org.mozilla.sysinfo.sysinfo)),
        ## Has current session info
        current = quote(is.numeric(r$data$last$org.mozilla.appSessions.current) || 
            is.list(r$data$last$org.mozilla.appSessions.current)),
        ## Has this ping date
        thisPingDate = quote(is.character(r$thisPingDate)),
        ## Has properly formatted dates
        valid.dates = quote(length(names(r$data$days)) == 0 || valid.dates(names(r$data$days))), 
        valid.thisPingDate = quote(valid.dates(r$thisPingDate)), 
        valid.lastPingDate = quote(is.null(r$lastPingDate) || valid.dates(r$lastPingDate))
    )
    
    fail.expr = quote(return(FALSE))
    if(count.fail)
        fail.expr = bquote({ 
                rhcounter("_CONDS_FAIL_", names(conds)[i], 1)
                .(fe)
            }, list(fe = fail.expr))
    
    check.valid = bquote({
            ## Check conditions progressively, since later conditions depend on earlier ones. 
            for(i in seq_along(conds)) {
                if(!eval(conds[[i]], 
                    list(r = r, valid.dates = valid.dates))) {
                    .(e)
                }
            }
            TRUE
        }, list(e = fail.expr)) 
    
    f = function(r) eval(check.valid)
    f.env = new.env(parent = globalenv())
    assign("conds", conds, envir = f.env)
    assign("check.valid", check.valid, envir = f.env)
    environment(f) = f.env
    f
}

## Check validity of strings intended to represent dates. 
## In FHR, the format should be yyyy-mm-dd.
## Returns TRUE if all elements of input are correctly formatted, FALSE otherwise.

valid.dates = function(d) {
    if(is.null(d) || length(d) == 0)
        return(NA)
    ## Use regex format check because as.Date doesn't enforce exact numbers of digits
    all(grepl("\\d{4}-\\d{2}-\\d{2}", d)) &&
            !any(is.na(as.Date(d, format = "%Y-%m-%d")))
}


## Generates conditions function to pass to query.fhr().
## Restrict to Mozilla FF, and non-NA architecture. 
## Also can specify whether to check for default channels and OSs (default TRUE). 
## In addition, can pass in conditions to check as a function which takes as input an FHR record and outputs a boolean. 
## As a shortcut, the logic function can refer directly to objects "gai" and "si" for geckoAppInfo and sysinfo respectively.

cond.default = function(logic, channel=TRUE, os=TRUE) {
    cond = list(quote(get.val(gai, "vendor") == "Mozilla"), 
        quote(get.val(gai, "name") == "Firefox"), 
        quote(!is.na(get.val(si, "architecture"))))
    
    if(channel) {
        cond[[length(cond) + 1]] = quote(  
            get.val(gai, "updateChannel") %in% c("nightly", "aurora", "beta", "release") 
        )
    }
    if(os) {
        cond[[length(cond) + 1]] = quote(
            get.val(gai, "os") %in% c("WINNT", "Darwin", "Linux")
        )
    }
    
    if(!missing(logic)) {
        if(!is.function(logic))
            stop("logic must be a function")
            
        f = eval(bquote(
            function(r) { 
                gai = r$geckoAppInfo
                si = r$data$last$org.mozilla.sysinfo.sysinfo
                ## Check default conditions first. 
                if(!do.call(all, .(cond))) 
                    return(FALSE)
                    
                ## Keep gai and si shortcuts in scope of logic function. 
                assign("gai", gai, environment(logic))
                assign("si", si, environment(logic))
                logic(r)
            }
        , list(cond = cond)))
        
        ## Retain logic function but none of the other local variables
        e = new.env(parent = parent.env(environment(f)))
        assign("logic", logic, e)
        environment(f) = e
        f
    } else {
        f = eval(bquote(
            function(r) { 
                gai = r$geckoAppInfo
                si = r$data$last$org.mozilla.sysinfo.sysinfo
                do.call(all, .(cond))
            }
        , list(cond = cond)))
        
        ## Don't need any local variables to be in scope
        environment(f) = parent.env(environment(f))
        f
    }   
}




