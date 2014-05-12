##############################################################
### 
###  Code to load FHR util functions. 
###  Will eventually be turned into a package. 
### 
##############################################################


for(f in list.files("fhr", pattern = "\\.R$")) {
    source(file.path("fhr", f), keep.source = FALSE)
}

