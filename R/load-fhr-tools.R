##############################################################
### 
###  Code to load FHR utility functions. 
###  Will eventually be part of a package. 
### 
##############################################################


local({
    # this.dir <- dirname(sys.frame(1)$ofile)
    #if(!exists("source2env", globalenv(), mode = "function", inherits = FALSE))
    #    source(file.path(this.dir, "other", "source-to-env.R"), local = TRUE)
    
    #source2env(file.path(this.dir, "fhr"), "fhrtools")
    fhr.dir <- file.path(dirname(sys.frame(1)$ofile), "fhr")
    for(f in list.files(fhr.dir, pattern = "\\.R$")) {
        source(file.path(fhr.dir, f), keep.source = FALSE)
    }
})

