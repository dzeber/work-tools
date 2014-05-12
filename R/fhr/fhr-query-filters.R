##############################################################
### 
###  Default and other relevant filter functions to use
###  with fhr.query(). 
### 
##############################################################



## Generates validity filter function for v2 FHR to pass to fhr.query(). 
## Will optional maintain counts of individual condition failures. 

fhr.v2.filter.gen = function(count.fail = FALSE) {
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
    
    f = eval(bquote(function(r) {
            ## Check conditions progressively, since later conditions depend on earlier ones. 
            for(i in seq_along(conds)) {
                if(!eval(conds[[i]], 
                    list(r = r, valid.dates = valid.dates))) {
                    .(e)
                }
            }
            TRUE
        }, list(e = fail.expr)))
    
    ## Keep only conds from current environment
    f.env = new.env(parent = parent.env(environment(f)))
    assign("conds", conds, envir = f.env)
    # assign("check.valid", check.valid, envir = f.env)
    ## Add this here while not using wrap.fun().
    assign("valid.dates", valid.dates, envir = f.env)
    environment(f) = f.env
    f
}


## Generates conditions function to pass to query.fhr().
## Restrict to Mozilla Firefox (vendor/name).
## Also can specify whether to check for standard channels and OSs (default FALSE), and whether to restrict to non-NA architecture (default FALSE). 
## In addition, can pass in conditions to check as a function which takes as input an FHR record and outputs a boolean. 
## As a shortcut, the logic function can refer directly to objects "gai" and "si" for geckoAppInfo and sysinfo respectively.

fhr.cond.default = function(logic, channel=FALSE, os=FALSE, arch.na=FALSE) {
    cond = list(quote(get.val(gai, "vendor") == "Mozilla"), 
        quote(get.val(gai, "name") == "Firefox"))
        
    if(arch.na) {
        cond[[length(cond) + 1]] = quote(!is.na(get.val(si, "architecture")))
    }
    
    if(channel) {
        cond[[length(cond) + 1]] = quote(  
            grepl("^(nightly|aurora|beta|release)", get.val(gai, "updateChannel"))
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
        environment(f) = new.env(parent = parent.env(environment(f)))
        f
    }   
}

