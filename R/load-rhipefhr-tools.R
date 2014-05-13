#######################################################################
### 
###  Code to load FHR and RHIPE utility functions. 
###  Will eventually be part of a package. 
### 
#######################################################################


local({
    this.dir <- dirname(sys.frame(1)$ofile)
    # if(!exists("source2env", globalenv(), mode = "function", inherits = FALSE))
        # source(file.path(this.dir, "other", "source-to-env.R"), local = TRUE)
    
    # source2env(file.path(this.dir, "fhr"), "fhrtools")
    # source2env(file.path(this.dir, "rhipe"), "rhipe.prefix")
    for(nm in c("fhr", "rhipe")) {
        curr.dir = file.path(this.dir, nm)
        for(f in list.files(curr.dir, pattern = "\\.R$")) {
            source(file.path(curr.dir, f), keep.source = FALSE)
        }
    }
})
