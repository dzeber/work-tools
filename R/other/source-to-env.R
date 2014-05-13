#######################################################################
###  
###  Function to source file or files from a directory into a named 
###  environment added to the search path. 
###
#######################################################################


## Creates a new environment with the specified name
## and adds it to the search path if it is not already there
## or if attach.first is FALSE.
## 
## If specified path is a directory, sources all contained .R files.
## Otherwise sources path as file. Assigns contents to the environment.
##
## Argument keep.source is passed FALSE. 

source2env <- function(src.path, env.name, attach.first = TRUE) {
    e <- if(env.name %in% search()) {
        as.environment(env.name)
    } else {
        if(attach.first) attach(NULL, name = env.name) else new.env()
    }
    if(file_test("-d", src.path)) {
        for(f in list.files(src.path, pattern = "\\.R$")) {
            sys.source(file.path(src.path, f), envir = e, keep.source = FALSE)
        }
    } else {
        sys.source(src.path, envir = e, keep.source = FALSE)
    }
    if(!attach.first) attach(e, name = env.name)
    rm(e, f)
}
