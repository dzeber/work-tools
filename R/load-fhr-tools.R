##############################################################
### 
###  Code to load FHR util functions to an environment 
###  on the search path named "fhrtools".
###  Will eventually be turned into a package. 
### 
##############################################################


local({
    fhr.dir <- file.path(dirname(sys.frame(1)$ofile), "fhr")
    e <- attach(NULL, name = "fhrtools")
    for(f in list.files(fhr.dir, pattern = "\\.R$")) {
        sys.source(file.path(fhr.dir, f), envir = e, keep.source = FALSE)
    }
})
