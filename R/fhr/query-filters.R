#######################################################################
### 
###  Generic filter functions to use with fhr.query(). 
###  
###  These include validity filters, to check for malformed packets, 
###  and conditions filters, to restrict packets according to some 
###  conditions on the variables. 
###  
###  Validity filters are created as a list named 'fhrfilter' with
###  elements 'v2' and 'v3'. These are functions which must be called 
###  to generate the filter.
###  
###  Conditions filter functions ('ff.cond.default' and 
###  'fennec.cond.default') are generic and can be augmented with 
###  additional logic. 
### 
#######################################################################


## Creates a generator function for a list of conditions passed as 
## named calls evaluating to booleans. 
## Output is a function that provides the option to maintain counts of 
## individual condition failures using rhcounter. 
## FHR packet should be referred to as 'r' in 'conds'.

wrap.conds <- function(conds) {
    function(count.fail = FALSE) {
        fail.expr <- quote(return(FALSE))
        if(count.fail)
            fail.expr <- bquote({ 
                rhcounter("_CONDS_FAIL_", names(conds)[i], 1)
                .(fe)
            }, list(fe = fail.expr))

        f <- eval(bquote(function(r) {
            ## Check conditions progressively, since later conditions depend on earlier ones. 
            env <- list(r = r, valid.dates = valid.dates)
            for(i in seq_along(conds)) {
                if(!eval(conds[[i]], env)) { .(e) }
            }
            TRUE
        }, list(e = fail.expr)))
        
        ## Keep only conds from current environment
        f.env <- new.env(parent = parent.env(environment(f)))
        assign("conds", conds, envir = f.env)
        # assign("check.valid", check.valid, envir = f.env)
        ## Add this here while not using wrap.fun().
        assign("valid.dates", valid.dates, envir = f.env)
        environment(f) <- f.env
        f
    }
}

fhrfilter <- list()

## Standard validity filter for v2 FHR packets (FF desktop) to pass to fhr.query.
fhrfilter$v2 <- wrap.conds(list(
    ## version 2 FHR
    fhr.version = quote(!is.null(r$version) && identical(r$version, 2)),
    ## Has geckoAppInfo - will be used to read app info
    geckoAppInfo = quote(is.character(r$geckoAppInfo) || is.list(r$geckoAppInfo)),
    ## Has data field
    data = quote(is.list(r$data)),
    ## Has info from last session
    last = quote(is.list(r$data$last)),
    ## Has days list
    days = quote(is.list(r$data$days)),
    ## Has system info
    sysinfo = quote(is.character(r$data$last$org.mozilla.sysinfo.sysinfo) || 
        is.list(r$data$last$org.mozilla.sysinfo.sysinfo)),
    ## Has current session info
    current = quote(is.numeric(r$data$last$org.mozilla.appSessions.current) || 
        is.list(r$data$last$org.mozilla.appSessions.current)),
    ## Has valid thisPingDate
    thisPingDate = quote(is.character(r$thisPingDate) && 
        valid.dates(r$thisPingDate)),
    ## lastPingDate is valid if present
    lastPingDate = quote(is.null(r$lastPingDate) || 
        (is.character(r$lastPingDate) && valid.dates(r$lastPingDate))), 
    ## Has properly formatted dates
    dates = quote(length(r$data$days) == 0 || 
        valid.dates(names(r$data$days))) 
))

## Standard validity filter for v3 FHR packets (Fennec).
fhrfilter$v3 <- wrap.conds(list(
    ## version 3 FHR
    fhr.version = quote(!is.null(r$version) && identical(r$version, 3)),
    ## Has environments list
    environments = quote(is.list(r$environments)), 
    ## Has current environment
    current = quote(is.list(r$environments$current)), 
    ## Has geckoAppInfo
    geckoAppInfo = quote((is.list(r$environments$current$geckoAppInfo) || 
        is.character(r$environments$current$geckoAppInfo))), 
    ## Has system info
    sysinfo = quote((is.list(r$environments$current$org.mozilla.sysinfo.sysinfo) ||
        is.character(r$environments$current$org.mozilla.sysinfo.sysinfo))), 
    ## Has data field
    data = quote(is.list(r$data)), 
    ## Has days list
    days = quote(is.list(r$data$days)), 
    ## Has valid thisPingDate
    thisPingDate = quote(is.character(r$thisPingDate) && 
        valid.dates(r$thisPingDate)), 
    ## lastPingDate is valid if present
    lastPingDate = quote(is.null(r$lastPingDate) || 
        (is.character(r$lastPingDate) && valid.dates(r$lastPingDate))), 
    ## Has properly formatted dates
    dates = quote(length(r$data$days) == 0 || 
        valid.dates(names(r$data$days))) 
))


#####


## Generates conditions function for FF desktop to pass to fhr.query().
## Restrict to profiles that are considered standard. 
## In addition, can restrict to release channel (releaseonly), Windows OS
## (windowsonly), and profiles with at least one active day (hasdaysonly).
## If either condition is not met by the payload, the conditions filter 
## will return a string that gets counted in the end.state table.

ff.cond.default <- function(releaseonly = FALSE, windowsonly = FALSE, 
                                                        hasdaysonly = FALSE) {
    conds <- list(
        quote(if(!is.standard.profile(r)) return("not standard profile")))
    if(releaseonly) {
        conds[[length(conds) + 1]] <- 
            quote(if(!on.release.channel(r)) return("not release"))
    }
    if(windowsonly) {
        conds[[length(conds) + 1]] <- 
            quote(if(!identical(get.standardized.os(r), "Windows")) 
                return("not windows"))
    }
    if(hasdaysonly) {
        conds[[length(conds) + 1]] <- 
            quote(if(length(r$data$days) == 0) return("no days"))
    }
    
    eval(bquote(function(r) {
        do.call(`{`, .(conds))
        TRUE
    }, list(conds = conds)))
}
# ff.cond.default <- function(logic, channel = FALSE, os = FALSE, arch.na = FALSE) {
    # cond <- list(quote(identical(get.val(gai, "vendor"), "Mozilla")), 
        # quote(identical(get.val(gai, "name"), "Firefox")))
        
    # if(arch.na) {
        # cond[[length(cond) + 1]] <- quote(!is.na(get.val(si, "architecture")))
    # }
    
    # if(channel) {
        # cond[[length(cond) + 1]] <- quote(  
            # grepl("^(nightly|aurora|beta|release)", get.val(gai, "updateChannel"))
        # )
    # }
    # if(os) {
        # cond[[length(cond) + 1]] <- quote(
            # get.val(gai, "os") %in% c("WINNT", "Darwin", "Linux")
        # )
    # }
    
    # if(!missing(logic)) {
        # if(!is.function(logic))
            # stop("logic must be a function")
            
        # f <- eval(bquote(
            # function(r) { 
                # gai <- r$geckoAppInfo
                # si <- r$data$last$org.mozilla.sysinfo.sysinfo
                # ## Check default conditions first. 
                # if(!do.call(all, .(cond))) 
                    # return(FALSE)
                    
                # ## Keep gai and si shortcuts in scope of logic function. 
                # assign("gai", gai, environment(logic))
                # assign("si", si, environment(logic))
                # logic(r)
            # }
        # , list(cond = cond)))
        
        # ## Retain logic function but none of the other local variables
        # e <- new.env(parent = parent.env(environment(f)))
        # assign("logic", logic, e)
        # environment(f) <- e
        # f
    # } else {
        # f <- eval(bquote(
            # function(r) { 
                # gai <- r$geckoAppInfo
                # si <- r$data$last$org.mozilla.sysinfo.sysinfo
                # do.call(all, .(cond))
            # }
        # , list(cond = cond)))
        
        # ## Don't need any local variables to be in scope
        # environment(f) <- new.env(parent = parent.env(environment(f)))
        # f
    # }   
# }


## Generates conditions function for Fennec to pass to query.fhr().
## Restrict to Mozilla fennec (vendor/name).
## Also can specify whether to check for standard channels (default FALSE) and OS (default FALSE). 
## In addition, can pass in conditions to check as a function which takes as input an FHR record and outputs a boolean. 
## As a shortcut, the logic function can refer directly to objects "gai" and "si" for geckoAppInfo and sysinfo respectively.

fennec.cond.default = function(logic, channel = FALSE, os = FALSE) {
    cond <- list(quote(identical(get.val(gai, "vendor"), "Mozilla")), 
        quote(identical(get.val(gai, "name"), "fennec")))
        
    if(channel) {
        cond[[length(cond) + 1]] <- quote(  
            grepl("^(nightly|aurora|beta|release|default)", 
                get.val(gai, "updateChannel"))
        )
    }
    if(os) {
        cond[[length(cond) + 1]] <- quote(identical(get.val(gai, "os"), "Android"))
    }
    
    if(!missing(logic)) {
        if(!is.function(logic))
            stop("logic must be a function")
            
        f <- eval(bquote(
            function(r) { 
                gai <- r$environments$current$geckoAppInfo
                si <- r$environments$current$org.mozilla.sysinfo.sysinfo
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
        e <- new.env(parent = parent.env(environment(f)))
        assign("logic", logic, e)
        environment(f) <- e
        f
    } else {
        f <- eval(bquote(
            function(r) { 
                gai <- r$environments$current$geckoAppInfo
                si <- r$environments$current$org.mozilla.sysinfo.sysinfo
                do.call(all, .(cond))
            }
        , list(cond = cond)))
        
        ## Don't need any local variables to be in scope
        environment(f) <- new.env(parent = parent.env(environment(f)))
        f
    }   
}


## Clean up
rm(wrap.conds)


