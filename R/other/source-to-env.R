#######################################################################
###  
###  Function to source file or files from a directory into a named 
###  environment added to the search path. 
###
#######################################################################


## Creates a new environment with the specified name
## and adds it to the search path if it is not already there. 
## 
## If specified path is a directory, sources all contained .R files.
## Otherwise sources path as file. Assigns contents to the environment.
##
## Argument keep.source is passed FALSE. 

source2env <- function(src.path, env.name) {
    if(env.name %in% search()) {
        e <- as.environment(env.name)
    } else {
        e <- attach(NULL, name = env.name)
    }
    if(file_test("-d", src.path)) {
        for(f in list.files(src.path, pattern = "\\.R$")) {
            sys.source(file.path(src.path, f), envir = e, keep.source = FALSE)
        }
    } else {
        sys.source(src.path, envir = e, keep.source = FALSE)
    }
    rm(e, f)
}
