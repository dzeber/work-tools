################################################################
###
###  Testing and examples for functions in rhipe-tools.R.
### 
################################################################


## Testing wrap.fun():
n = 2
g = function(a) {
    p = 2
    r = 3
    fun = function(b) {
        ## b + 2
        b + p
    }
    function(d) {
        ## Contents of enclosing environment
        print("Before:")
        print(ls(env = parent.env(environment())))
        f = function(j) {
            ## (3 + 2) + a + d + 2 + j
            fun(r) + a + d + n + j
        }
        f = wrap.fun(f)
        rm(d, a, fun, r, n, p, inherits = TRUE)
        print("After:")
        print(ls(env = parent.env(environment())))
        f
    }
}

## gg = 7 + 4 + 1 + j
gg = g(4)(1)

## 12
gg(0)


