##############################################################
### 
###  Code to load general utility functions to an environment 
###  on the search path named "worktools".
###  Will eventually be part of a package. 
### 
##############################################################


local({
    # this.dir <- dirname(sys.frame(1)$ofile)
    # if(!exists("source2env", globalenv(), mode = "function", inherits = FALSE))
        # source(file.path(this.dir, "other", "source-to-env.R"), local = TRUE)
    
    # source2env(file.path(this.dir, "general"), "worktools")
    wt.dir <- file.path(dirname(sys.frame(1)$ofile), "general")
    for(f in list.files(wt.dir, pattern = "\\.R$")) {
        source(file.path(wt.dir, f), keep.source = FALSE)
    }
})
