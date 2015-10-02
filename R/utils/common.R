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

## Print a named vector as a data table for clarity.
pv <- function(v) {
    data.table(names = names(v), vals = v)
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
## It should be called at the beginning of a script to create a handler 
## function to be passed to tryCatch(). When used in tryCatch, the handler 
## should be supplied a descriptive message indicating where/why the error 
## occurred for reporting. The result of evaluating the handler is itself a 
## function taking the error as a single argument. 
## 
## Example usage:
## myerrorfn <- scriptErrorHandler()
## tryCatch({ ... }, error = myerrorfn("Script block failed"))
## 
## The custom message reported by this handler includes the specified 
## description. 
## If 'appendmsg' is TRUE, the error message will be appended to the 
## description. 
## If 'appendcall' is TRUE, the call that caused the error will be appended. 
## Otherwise, only the description will be reported.
## If 'sendemail' is TRUE, an email message will be sent using mailx to an 
## address read from the SCRIPT_ERROR_EMAIL environment variable, if any.
## Finally, further execution is halted, reporting a message including the 
## customized description.
scriptErrorHandler <- function(appendmsg = TRUE, appendcall = TRUE, 
                        sendemail = TRUE, jobdescription = "Script failed!") {
    handler <- list(
        quote(`{`),
        ## Join the error message to the user-provided description, if required.
        if(appendmsg) { 
            quote(description <- sprintf("%s: %s", .(errdesc), error$message))
        } else { 
            quote(description <- .(errdesc)) 
        })
    ## Append the call that caused the error, if required.
    if(appendcall) {
        template <- sprintf("%%s %s %%s", if(appendmsg) "in" else "at")
        handler[[length(handler) + 1]] <- substitute(
            description <- sprintf(template, description, 
                deparse(error$call)),
            list(template = template))
    }
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
    handler <- as.call(handler)
    ## Create a factory function that returns this error handling function
    ## with the user-provided description substituted in.
    function(description) {
        handler <- eval(substitute(
            bquote(hbody, list(errdesc = description)), 
            list(hbody = handler)))
        handlerfn <- function(error) {}
        body(handlerfn) <- as.call(handler)
        handlerfn
    }
}

