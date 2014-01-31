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

rm(n, g, gg)

a = 2
b = 3
f1 = function(r) { a + r }
f2 = function(s) { b + s }
X = list(fn1 = f1, fn2 = f2)
X = wrap.fun(X)
## returns list(fn1 = "a", fn2 = "b")
lapply(lapply(X, environment), ls)



