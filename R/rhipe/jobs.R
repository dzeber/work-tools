#######################################################################
###  
###  Utility functions for running RHIPE jobs on hala.
###  
#######################################################################


## Sets up directories for working on hala. 
##
## Creates local subdir of home dir on hala and switches to it. 
## Returns function that will generate matching paths on hala 
## leading to specified subdirs. 
##
## Specify base name of working dir to create, as well as optional subpath 
## (relative to home dir). Default subpath is "fhr". 
## Also includes boolean flag indicating whether or not to return HDFS function. 

set.dir = function(base.name, path = "fhr", hdfs.fun = TRUE) {
    subpath = file.path(path, base.name)
    local.path = file.path("~", subpath)
        
    ## Create path on local if doesn't exist. 
    system(sprintf("mkdir -p %s", local.path))
    ## Switch to new base dir. 
    setwd(local.path)
    
    ## Create function to generate HDFS paths. 
    if(hdfs.fun) {
        eval(bquote(function(dir.name) {
            file.path(.(p), dir.name)
        }, list(p = file.path("/user", Sys.getenv("USER"), subpath))))
    }
}


