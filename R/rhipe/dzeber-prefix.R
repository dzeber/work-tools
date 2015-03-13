###########################################################
###
###  Custom code to run on starting R
###
###########################################################

## Helpful hint to clean up
local({
  x <- sum(grepl("/rhipe-temp-",rhls(rhoptions()$HADOOP.TMP.FOLDER)$file))
  if(x > 50) warning(sprintf("There are %s temporary files, use rhclean?",x))
})

## Redefine RHIPE MR functions for local testing. 
rhcollect <- function(x,y) { 
    ## Format key and value as data tables for convenient printing.
    if(!is.data.table(x)) x <- as.data.table(as.list(x))
    if(!is.data.table(y)) y <- as.data.table(as.list(y))
    ## If both are single-row, merge.
    entry <- if(max(nrow(x), nrow(y)) == 1) cbind(x, y) else list(x,y)
    print(entry) 
}
    
rhcounter <- function(x,y,n) { 
    print(sprintf("%s|%s: %s", x, y, n)) 
}

## Add exclusion for rhcounter.
co <- rhoptions()$copyObjects
if(!("rhcounter" %in% co$exclude)) 
    co$exclude[length(co$exclude) + 1] <- "rhcounter"
rhoptions(copyObjects = co)
rm(co)

## Alias for colsummer template
colsummer <- rhoptions()$templates$colsummer


## Convert NULLs to NA - currently loaded by site prefix
#isn=function(r) if(is.null(r) || length(r)==0) NA else r

# dateSequence <- function(dates,format="%Y%m%d"){
  # e1 <- dates[1];e2 <- dates[2]
  # if(e1==e2) return(rep(e1,2))
  # e1 <- as.Date(e1,format)
  # e2 <- as.Date(e2,format)
   # strftime(e1 + 0:(e2-e1),format)
# }


## Windows Version Numbering
# winVerCheck <- function(ver){
  # ## http://www.msigeek.com/442/windows-os-version-numbers
  # mu <- sapply(c("win7"="6.1","winVista"="6.0",'winXP'="5.1")
               # ## ,"win2K"="5.0","winMe"="4.9", "win98"="4.1","win95"="4.0","winNT"="3.5")
  # ,function(r) sprintf("^(%s)",r))
  # mun <- names(mu)
  # id <- 1:length(mu)
  # return(function(s){
    # if(is.na(s) || is.null(s) || length(s)==0) return(NA)
    # for(i in id){
      # if(grepl(mu[i],s)) return(mun[i])
    # }
    # return("Others")
  # })
# }


## Wrappping Expressions for Debug
# dbgExpression <- function(r,throwError=FALSE){
  # .r <- substitute(r)
  # r <- if( is(.r,"name")) get(as.character(.r)) else .r
  # if(!throwError)
    # bquote(
           # tryCatch(.(r)
                    # , error = function(e) { rhcounter("R_UNTRAPPED_ERRORS",as.character(e),1); e}
                    # , warning = function(e) { rhcounter("R_UNTRAPPED_WARNINGS",as.character(e),1);}
                    # , message = function(e) { rhcounter("R_MESSAGES",as.character(e),1);})
           # ,list(r=r))
  # else 
    # bquote(tryCatch(.(r)
                    # , error = function(e) stop(e)
                    # , warning = function(e) {rhcounter("R_UNTRAPPED_WARNINGS",as.character(e),1);}
                    # , message = function(e) {rhcounter("R_MESSAGES",as.character(e),1);})
           # ,list(r=r))
# }


##' Takes seconds and converts to human readable format
##' @param secs is the number of seconds
##' @return a string
# secondsToString <- function(secs,rnd=2){
  # Round <- function(a,b){
    # format(round(a,b),nsmall=2)
  # }
  # if(secs<60) sprintf("%s seconds",secs)
  # else if(secs<60*60) sprintf("%s minutes",Round(secs/60,rnd))
  # else if(secs< 86400) sprintf("%s hours", Round(secs/(60*60),rnd))
  # else if(secs< (86400*30)) sprintf("%s days",Round(secs/(86400),rnd))
  # else if(secs< (86400*365)) sprintf("%s months",Round(secs/(86400*30),rnd))
  # else  sprintf("%s years",Round(secs/(86400*365),rnd))
# }


