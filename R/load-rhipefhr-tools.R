#######################################################################
### 
###  Code to load FHR and RHIPE utility functions to environments 
###  on the search path. 
###  Evnironments will be named "fhrtools" and "rhipe.prefix" 
###  respectively. 
###  Will eventually be part of a package. 
### 
#######################################################################


local({
    this.dir <- dirname(sys.frame(1)$ofile)
    if(!exists("source2env", globalenv(), mode = "function", inherits = FALSE))
        source(file.path(this.dir, "other", "source-to-env.R"), local = TRUE)
    
    source2env(file.path(this.dir, "fhr"), "fhrtools")
    source2env(file.path(this.dir, "rhipe"), "rhipe.prefix")
})
