#######################################################################
###  
###  Convenience functions for use on hala or locally.
###  
#######################################################################


## Wrap referenced objects into a list.
## List names are the corresponding object names, 
## unless custom names are supplied.
param.list <- function(..., .names) {
    a <- substitute(list(...))[-1]
    a <- unlist(lapply(a, as.character))
    an <- a
    if(!missing(.names) && !is.null(names(.names))) {
        rn <- an %in% names(.names)
        if(any(rn)) {
            an[rn] <- .names[an[rn]]
        }
    }
    setNames(lapply(a, get), an)
}

## Convert a vector of numbers to their relative percentages 
## of the total.
## Rounded to the specified number of decimals.
## To skip rounding, use decimals = NULL.
to.pct <- function(x, decimals = 3) {
    pct <- x / sum(x) * 100
    if(is.null(decimals)) return(pct)
    round(pct, decimals)
}

## Trim whitespace from both ends of string. 
trim <- function(x) {
    gsub("^\\s+|\\s+$", "", x)
}

## Add commas to long numbers for easier reading in a printed data table.
## (This converts them to character).
longnum <- function(x) {
    prettyNum(x, big.mark = ",", scientific = FALSE)
}

## Print an entire data table regardless of size.
pdt <- function(DT) {
    print(DT, nrows = nrow(DT))
}

## Print the elements of a vector, possibly named, as rows in a data table for 
## clarity.
pv <- function(v) {
    cols <- list()
    if(!is.null(names(v))) cols[["names"]] <- names(v)
    cols[["vals"]] <- v
    do.call(data.table, cols)
}


## Status messaging formatted to include current system time. 
## Useful for status updates inside scripts.
statusMessage <- function(msg) {
    message(sprintf("%s at %s", 
        msg, format(Sys.time(), "%H:%M:%S %Z on %Y-%m-%d")))
}


## Create an error handler that reports a custom error message and stops
## further execution, optionally sending an email notification. 
## 
## This function should be called at the beginning of a script to generate an 
## error handler to be used in the script. The error handler is a function 
## that can either be called directly to halt execution with a custom message,
## or used to create a handler for tryCatch(). In either case, it should be 
## supplied a descriptive message indicating where/why the error occurred, for 
## diagnostic purposes.
## 
## The error handler returned by this function takes two arguments, the error 
## description and 'catch', a boolean indicating whether or not the the handler
## is to be used to listen for errors in tryCatch. If TRUE, calling the error
## handler function returns another function which takes an error as its
## only arg and applies the error handling logic to the error, if any occurs. 
## The default values of the 'catch' arg is FALSE.
## 
## Example usage:
## kill <- scriptErrorHandler()
## ## If bad data is detected: report a custom message and halt execution.
## if(...) kill("Unable to process input data")
## ## Inside tryCatch:
## tryCatch({ ... }, error = kill("Try block failed", catch = TRUE))
## 
## The custom message reported by this handler includes the specified 
## description. 
## If 'appendmsg' is TRUE, when used in tryCatch, the error message will be 
## appended to the description. 
## If 'appendcall' is TRUE, when used in tryCatch, the call that caused the 
## error will be appended. 
## If both of these are FALSE, only the supplied description will be reported.
## If 'sendemail' is TRUE, an email message will be sent using mailx to an 
## address read from the SCRIPT_ERROR_EMAIL environment variable, if any. 
## The value of the 'jobdescription' arg will be used as the email subject.
scriptErrorHandler <- function(appendmsg = TRUE, appendcall = TRUE, 
                        sendemail = TRUE, jobdescription = "Script failed!") {
    ## Code to generate the message when passed an error.
    errormsg <- quote(.(desc))
    ## Join the error message to the user-provided description, if required.
    if(appendmsg) { 
        errormsg <- bquote(sprintf("%s: %s", .(msg), error$message),
            list(msg = errormsg))
    }
    ## Append the call that caused the error, if required.
    if(appendcall) {
        template <- sprintf("%%s %s %%s", if(appendmsg) "in" else "at")
        errormsg <- bquote(sprintf(.(template), .(msg), deparse(error$call)),
            list(template = template, msg = errormsg))
    }
    errormsg <- bquote(description <- .(msg), list(msg = errormsg))
            
    ## Code to handle the error.
    handler <- list()
    ## Send an email notification if required.
    if(sendemail) {
        ## Look up the email address.
        ## Only try to send the email if a valid email address is found.
        emailaddress <- grep("@", Sys.getenv("SCRIPT_ERROR_EMAIL"), 
            value = TRUE, fixed = TRUE)
        if(length(emailaddress) > 0) {
            emailcmd <- sprintf("echo '%%s' | mailx -s '%s' %s", 
                jobdescription, emailaddress)
            handler[[length(handler) + 1]] <- substitute(
                system(sprintf(cmd, description)), list(cmd = emailcmd))
        }
    }
    ## Finally, stop execution with a customized message.
    handler[[length(handler) + 1]] <- quote(
        stop(sprintf("*** %s. Exiting...", description), call. = FALSE))
    
    ## Create the error handling function that either runs the handler directly, 
    ## if 'catch' is FALSE, or generates a function to run the handler, 
    ## incorporating the error message, if 'catch' is TRUE.
    function(description, catch = FALSE) {
        if(catch) {
            errorfn <- function(error) {}
            errorhandler <- c(list(quote(`{`),
                ## Incorporate error information into the description.
                eval(substitute(bquote(msg, list(desc = description)), 
                    list(msg = errormsg)))),
                handler)
            body(errorfn) <- as.call(errorhandler)
            environment(errorfn) <- baseenv()
            return(errorfn)
        }
        ## Run the handler on the description passed to the function.
        eval(as.expression(handler))
    }
}


## Parse command line args passed to a script. This is intended to be called in 
## scripts run at the command line using Rscript. 
## 
## Parses out the --file arg (useful for finding the dir containing the script 
## that is running) and any user-supplied args. 
##
## Args are returned in a list, in the same order as they were supplied.
## If any args are key-value pairs of the form "--key=value", the corresponding 
## list element will contain the value and the name for that element will be 
## the key. 
## Otherwise the list element will be unnamed and contain the full arg string, 
## including any "-" characters. 
## 
## The returned list may have some named elements and some unnamed, and
## should contain an element named "file" with the script path.
parseCommandArgs <- function() {
    parsedargs <- list()
    ## Get all args, since we want to see the --file arg.
    rawargs <- commandArgs(trailingOnly = FALSE)
    ## Extract the --file arg first.
    filearg <- grep("^--file=", rawargs, value = TRUE)
    if(length(filearg) > 0) 
        parsedargs[["file"]] <- sub("--file=", "", filearg)
    ## If any custom args are supplied, they will follow the "--args" argument.
    ## Otherwise the "--args" argument will not be present.
    ## In other words, if it is present, it should occur before the last arg.
    customflag <- which(rawargs == "--args")
    if(length(customflag) > 0 && min(customflag) < length(rawargs)) {
        ## Keep everything after the first occurrence of the flag.
        customargs <- rawargs[(min(customflag) + 1) : length(rawargs)]
        customargs <- unlist(lapply(customargs, function(aa) {
            ## For key-value pairs, return the value named by the key.
            ## Otherwise return the string as is.
            argparts <- regmatches(aa, regexec("^--([^=]+)=(.*)$", aa))[[1]]
            if(length(argparts) > 0)
                setNames(list(argparts[[3]]), argparts[[2]])
            else
                list(aa)
        }), recursive = FALSE)
        parsedargs <- c(parsedargs, customargs)
    }
    parsedargs
}
