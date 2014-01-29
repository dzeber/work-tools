##############################################################
### 
###  Code to extract and format FHR data. 
### 
##############################################################


## Run query to extract FHR data from samples. 
## Optionally returns subsample. 
##
## Parameters: 
##
## output.folder - the HDFS folder to store output to. Can be NULL.
##
## -- Query content --
## logic - the code to apply to valid FHR packets meeting the conditions. 
##    * Should be a function taking a record key and an associated FHR packet.
##    *   eg. function(k,r) { res = ...; rhcollect(k,res) }
##    * Must include rhcollect statement for output to be emitted. 
##    * If unspecified, does nothing.
## valid.filter - checks whether or not an FHR packet is valid. 
##    * Should be a function taking an FHR packet and evaluating to a boolean.
##    * Defaults to a sensible check function. 
##    * If explicitly NULL, filter matches all records. 
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
## reduce - the reducer to apply 
## param - additional parameters to pass to RHIPE job
## debug - debugging handler for RHIPE job 
##
## Outputs the job handle z. Counters can be accessed using z$stats and the total number of outputted records is accessible as z$count. 

fhr.query = function(output.folder = NULL
                    ,logic = NULL
                    ,valid.filter = ff.valid.filter.v2
                    ,conditions.filter = NULL
                    ,prop = NULL
                    ,num.out = NULL
                    ,reduce = NULL
                    ,param = list()
                    ,mapred = NULL
                    ,debug = "count"
                ) {
    
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
            valid.filter 
        }    
 
## Not updated yet:
 
    # param[["meets.conditions"]] = 
        # if(is.null(conditions.filter)) { function(r) { TRUE } } else { conditions.filter }
        
    # ## If num.out is given, compute proportion to give target number of output records. 
    # ## If both prop and num.out are specified, prop wins. 
    # if(is.numeric(num.out) && !is.na(num.out) && length(num.out) == 1 && num.out > 0 
            # && is.na(isn(prop))) {
        # ## Run preliminary job to count records. 
        # cj = rhwatch(map=rhmap({ 
                    # r = fromJSON(rawToChar(r[[1]]))
                    # if(is.valid.packet(r) && meets.conditions(r))
                        # rhcounter("_STATS_", "NUM_RECORDS", 1)    
                # }, before={ library(RJSONIO) })
                # ,input=hbaseif(table="metrics", colspec="data:json")
                # # ,output=rhoptions()$ioformats$null()
                # ,param=param
                # ,debug="count"
                # ,read=FALSE
        # )
        # n.records = cj[[1]][[c("counters","_STATS_")]][["NUM_RECORDS",1]]
        # if(is.na(n.records) || n.records == 0) 
            # stop("No matching records found.")
        # prop = num.out / n.records
    # }            
    # param[["retain.current"]] = 
        # if(is.numeric(prop) && !is.na(prop) && length(prop) == 1 && prop > 0 && prop < 1) { 
            # eval(substitute(function() { runif(1) < prop }, list(prop=prop)))
        # } else {
            # function() { TRUE }
        # }
        
    # param[["process.record"]] = 
        # if(is.null(logic)) { function(k, r) { } } else { logic }
                        
    # m = function(k,r) {
        # rhcounter("_STATS_", "NUM_PROCESSED", 1)
        # packet = tryCatch({
            # ## Try parsing the JSON payload. 
            # fromJSON(r)
        # },  error=function(err) { NULL })
        
        # if(is.null(packet)) {
            # ## There was a problem parsing. 
            # rhcounter("_STATS_", "BAD_JSON", 1)
        # } else {
            # rhcounter("_STATS_", "JSON_OK", 1)
            # ## Check validity of data record. 
            # ## NAs are treated as false. 
            # valid = is.valid.packet(packet)
            # if(ifelse(is.na(valid), FALSE, valid)) {
                # rhcounter("_STATS_", "VALID_RECORD", 1)
                # ## Check if conditions are met. 
                # cond = meets.conditions(packet)
                # if(ifelse(is.na(cond), FALSE, cond)) {
                    # rhcounter("_STATS_", "MEET_CONDITIONS", 1)
                    # ## Include current record at random. 
                    # if(retain.current()) {
                        # rhcounter("_STATS_", "NUM_RETAINED", 1)
                        # process.record(k, packet)
                    # }
                    # else {
                        # rhcounter("_STATS_", "NUM_EXCLUDED", 1)
                    # }
                # } else {
                    # rhcounter("_STATS_", "NOT_MEET_CONDITIONS", 1)
                # }
            # } else { 
                # rhcounter("_STATS_", "INVALID_RECORD", 1)
            # } 
        # }
    # }
    
    # z = rhwatch(map = m 
                # ,reduce = reduce
                # ,input = sqtxt("/user/sguha/fhr/samples/output/5pct")
                # # ,input=sqtxt("/data/fhr/nopartitions/20130811/")
                # ,output = output.folder
                # ,param = param
                # ,setup = expression(map = { library(RJSONIO) })
                # ,mapred = mapred
                # ,debug = debug
                # ,read = FALSE
    # )
                      
    # if(z[[1]]$rerrors || z[[1]]$state=="FAILED") {
        # warning("Job did not complete sucessfully.")
        # return(z)
    # }
    
    # ## Format counters, if available. 
    # zz = tryCatch({
        # stats = z[[1]][[c("counters","_STATS_")]]
        # if(!is.null(stats)) {
            # ## Reorder counters in hierarchical order (from alphabetical). 
            # ctrs = c("NUM_PROCESSED", "JSON_OK", "VALID_RECORD", "MEET_CONDITIONS",
                # "NUM_RETAINED", "NUM_EXCLUDED", "NOT_MEET_CONDITIONS", "INVALID_RECORD", "BAD_JSON")
            # ctrs = ctrs[ctrs %in% rownames(stats)]
            # stats = as.matrix(stats[ctrs,])
            # z[["stats"]] = stats
            # z[["count"]] = ifelse(!is.na(stats[["NUM_RETAINED",1]]), stats[["NUM_RETAINED",1]], 0)
        # }
        # z
    # }, error=function(err) { NULL })
    # if(!is.null(zz))
        # z = zz
    # z
}


# ## A standard validity filter to use for FF FHR (version 1) data. 

# ff.valid.filter = function(r) {
    # return(
        # ## Has data field
        # is.list(r$data)
        # ## Has info from last session
        # && is.list(r$data$last)
        # ## Has days field
        # && is.list(r$data$days)
        # ## Has current app info
        # ## Partial matching using '$' operator will match *.1 and *.2 as well
        # && is.character(r$data$last$org.mozilla.appInfo.appinfo) || 
            # is.list(r$data$last$org.mozilla.appInfo.appinfo)
        # ## Has system info
        # && is.character(r$data$last$org.mozilla.sysinfo.sysinfo) || 
            # is.list(r$data$last$org.mozilla.sysinfo.sysinfo)
        # ## Has current session info
        # && is.numeric(r$data$last$org.mozilla.appSessions.current) || 
            # is.list(r$data$last$org.mozilla.appSessions.current)
    # )
# }


# ## Validity filter for v2 FHR. 

# ff.valid.filter.v2 = function(r) {
    # return(
        # ## Has data field
        # is.list(r$data)
        # ## Has info from last session
        # && is.list(r$data$last)
        # ## Has days field
        # && is.list(r$data$days)
        # ## Has current app info
        # # && is.character(r$data$last$org.mozilla.appInfo.appinfo) || 
            # # is.list(r$data$last$org.mozilla.appInfo.appinfo)
        # ## Has system info
        # && is.character(r$data$last$org.mozilla.sysinfo.sysinfo) || 
            # is.list(r$data$last$org.mozilla.sysinfo.sysinfo)
        # ## Has current session info
        # && is.numeric(r$data$last$org.mozilla.appSessions.current) || 
            # is.list(r$data$last$org.mozilla.appSessions.current)
        # ## Has geckoAppInfo - will be used to read app info
        # && is.character(r$geckoAppInfo) || is.list(r$geckoAppInfo)
        # ## version 2 FHR
        # && !is.null(r$version) && r$version == 2
    # )
# }


# ## Generates conditions function to pass to query.fhr().
# ## Restrict to Mozilla FF, and optionally standard channels and OSs.

# cond.default = function(channel=TRUE, os=TRUE) {
    # cond = list(quote({ get.val(gai, "vendor") == "Mozilla"}), 
        # quote({ get.val(gai, "name") == "Firefox" }))
    # if(channel) {
        # cond[[length(cond) + 1]] = quote({  
            # get.val(gai, "updateChannel") %in% c("nightly", "aurora", "beta", "release") 
        # })
    # }
    # if(os) {
        # cond[[length(cond) + 1]] = quote({ 
            # get.val(gai, "os") %in% c("WINNT", "Darwin", "Linux")
        # })
    # }
    
    # function(r) { 
        # gai = r$geckoAppInfo
        # all(unlist(lapply(cond, function(r) {
            # eval(r, list(gai=gai))
        # })))
    # }
# }


# ## Robust accessor for FHR values. 
# ## Retrieves the element with name n from data list/vector d.
# ## Returns NA if no such element. 

# get.val = function(d, n) { isn(d[n][[1]]) }




