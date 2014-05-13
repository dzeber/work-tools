#######################################################################
###  
###  Function to load files from a directory into a named 
###  environment added to the search path. 
###
#######################################################################


## Creates a new environment with the specified name
## and adds it to the search path if it is not already there. 
## Sources all R source files in specified directory and 
## assigns contents to the environment.
## Argument keep.source is passed FALSE. 

source2env <- function(src.dir, env.name) {
    if(env.name %in% search()) {
        e <- as.environment(env.name)
    } else {
        e <- attach(NULL, name = env.name)
    }
    for(f in list.files(src.dir, pattern = "\\.R$")) {
        sys.source(file.path(src.dir, f), envir = e, keep.source = FALSE)
    }
    rm(e, f)
}
