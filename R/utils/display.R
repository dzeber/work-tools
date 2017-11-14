#######################################################################
###  
###  Convenience functions for formatting and displaying information.
###  
#######################################################################


## Format large numbers as a string, using the comma to separate thousands.
## Optionally, round to the specified number of digits.
## This rounds to the nearest 10^(-digits), where digits can be negative.
bigNum <- function(nums, digits = NULL) {
    if(!is.null(digits))
        nums <- round(nums, digits = digits)
    prettyNum(nums, big.mark = ",", scientific = FALSE)
}
## Fix for required long number formatting function.
longnum <- bigNum

## Convert proportions to a percentage formatted as a string.
## Specify the number of decimal places to include.
pctLabelText <- function(props, dec = 1) {
    sprintf(sprintf("%%.%df%%%%", dec), props * 100)
}

## Append counts to label strings.
## Creates a named vector mapping label strings to new label strings with
## counts appended.
countLabel <- function(lab, n) {
    setNames(sprintf("%s (%s)", lab, n), lab)
}

## Format a count and percent to be displayed together as a string.
countPct <- function(n, p) {
    sprintf("%s (%s)", bigNum(n), pctLabelText(p))
}


## Print summary output for a linear model.
## Includes the usual coefficients table, the ANOVA table, and AIC/BIC,
## all toggled by parameter switches.
## For ANOVA, supply the type number or NULL to omit.
printModelSummary <- function(fit, anova_type = 2, coefs = TRUE, ic = TRUE) {
    if(coefs) print(summary(fit))
    if(!is.null(anova_type)) {
        if(anova_type == 1)
            print(anova(fit, test = "F"))
        else {
            library(car, quietly = TRUE)
            print(Anova(fit, type = anova_type, test = "F"))
        }
    }
    if(ic) cat(sprintf("\nAIC: %.0f  |  BIC: %.0f\n", AIC(fit), BIC(fit)))
}


## Print the coefficients table as returned by print(summary(lm(...))).
## Optionally specify a vector of coefficient names to include.
printCoefTable <- function(fit, coefs = NULL) {
    fitsumm <- summary(fit)
    if(length(coefs) > 0) {
        ## Update the coefficients object first, so that only these rows
        ## will get printed with the formatting.
        fitsumm$coefficients <- fitsumm$coefficients[coefs, , drop = FALSE]
    }
    summpr <- capture.output(print(fitsumm))
    summpr <- summpr[
        grep("^Coefficients:", summpr) : grep("Signif. codes:", summpr)]
    cat(do.call(multiline, as.list(summpr)))
}


## Print a table of the top most common levels of a vector of grouping variables
## in a data table, together with row count, percentage and optionally
## cumulative percent.
## The table can either be ordered by decreasing count, in which case the
## default is to show the top 10 groupings, or by the grouping levels
## themselves, in which case the default is to show all groups.
## Optionally indicate a type label for the rows as `rowstr`, eg. "profiles"
## or "subsessions".
groupCounts <- function(DT, groupingcols, ntop = if(orderbyN) 10 else NA,
                            orderbyN = TRUE, rowstr = "rows", cumpct = TRUE) {
    topvals <- DT[, .N, keyby = groupingcols][,
        sprintf("num %s", rowstr) := bigNum(N)][,
        sprintf("pct %s", rowstr) := pctLabelText(N / sum(N))]
    if(orderbyN)
        topvals <- topvals[order(-N)]
    if(cumpct)
        topvals <- topvals[, "cum pct" := pctLabelText(cumsum(N) / sum(N))]
    topvals <- topvals[, N := NULL]
    if(!is.na(ntop))
        topvals <- topvals[1:min(.N, ntop)]
    topvals[]
}


## Shortcut to groupCounts() for the case where we have a small number of
## groups which we want to show in their entirety orderded by the levels of the
## grouping factor.
factorCounts <- function(DT, groupingcols, rowstr = "rows") {
    groupCounts(DT, groupingcols, ntop = NA, orderbyN = FALSE, rowstr = rowstr,
        cumpct = FALSE)
}


## Wrapper for inline Rmarkdown code chunks (currently needs to be called
## manually).
## If the code fails, eg. because a variable is not yet defined, print some
## placeholder text.
## Optionally specify a vector of chunk names that the inline code depends on.
## Their caches will be loaded prior to evaluating the chunk.
## Currently, the cache dir needs to be specified explicitly.
inline <- function(expr, dependent_chunks = NULL,
                                            cache_dir = params$cache_dir) {
    tryCatch({
        if(!is.null(dependent_chunks) && !is.null(cache_dir)) {
            cache_dbs <- list.files(cache_dir, pattern = "\\.rdb$")
            cached <- grep(
                sprintf("^%s", paste(dependent_chunks, collapse = "|")),
                cache_dbs,
                value = TRUE)
            if(length(cached) > 0) {
                cached <- sub("\\.rdb$", "", cached)
                for(cf in cached)
                    lazyLoad(file.path(cache_dir, cf), envir = globalenv())
            }
        }
        expr
    }, error = function(e) { "__\\*\\*R inline\\*\\*__" })
}



