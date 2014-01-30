################################################################
###
###  Useful functions for working with RHIPE. 
### 
################################################################



## Generate closure containing non-local objects referenced in a function.
## Useful when passing a function as a param to RHIPE that refers to other objects in the global environment. 
##
## Takes a function as input;
## returns the same function with its environment modified so that it 
## should run irrespective of what objects are present in the global environment. 
 
wrap.fun = function(f) {
    require("codetools")
    
    g = findGlobals(f, merge = FALSE)
    ## Output is list(functions=, variables=)
    
    ## Check for objects that are not defined anywhere in the search path. 
    v.def = lapply(g, function(v) {
        list(name = v, def = sapply(v, exists, envir = environment(f)))
    })
    
    if(!all(unlist(lapply(v.def, "[[", "def")))) {
        g = lapply(v.def, function(v) {
            if(all(v$def)) return(v$name)
            ## Warn and discard. 
            warning(sprintf("no visible binding for: %s", 
                paste(v$name[!v$def], collapse = ", ")), call. = FALSE)
            v$name[v$def]
        })
    }
    
    ## Retain only "user-defined" objects referenced in f. 
    ## These are objects belonging to any environment going back to and including the global environment.
    v.env = lapply(g, function(v) {
        e = lapply(v, codetools:::findOwnerEnv, 
                env = environment(f), stop = parent.env(globalenv()))
        ind = which(!is.na(e))
        lapply(ind, function(i) { list(name = v[i], env = e[[i]]) })
    })
      
    ## If there are no user-defined objects referenced, return the original closure. 
    if(max(sapply(v.env, length)) == 0) return(f)
          
    ## Create a new environment for f containing these objects
    ## If f is originally a closure, any enclosed objects will still be included here. 
    env.f = new.env(parent = globalenv())
    for(r in v.env$variables) 
        assign(r$name, get(r$name, envir = r$env, inherits = FALSE), 
            envir = env.f)
    ## Recurse on any referenced functions to enclose their referenced objects. 
    for(r in v.env$functions)
        assign(r$name, wrap.fun(get(r$name, envir = r$env, inherits = FALSE)), 
            envir = env.f)
            
    environment(f) = env.f
    f
}

