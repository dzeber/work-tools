#######################################################################
###  
###  GGPlot customizations.
###  
#######################################################################


require(scales)


## Multipurpose number formatting that covers a few select cases.
##
## If 'digits' is specified, numbers will be rounded to the nearest
## 10^(-digits). To round to the nearest (positive) power of 10, use a negative
## value for digits.
## If 'fixed' is TRUE, the formatted numbers will have exactly that many digits
## after the decimal point, with trailing 0s added as needed. Otherwise, the
## formatted numbers will have at most that many digits after the decimal (eg.
## fewer if all original numbers got rounded to whole numbers). If 'digits' is
## not given, 'fixed' is ignored.
##
## If 'padtrailing' is TRUE, trailing 0s will be added as needed to make all
## numbers have the same number of digits after the decimal point. If 'digits'
## is given, this will be applied to the rounded numbers. This is different
## from using 'fixed = TRUE', since the rounded numbers may have fewer digits
## after the decimal point.
##
## If 'thousands' is TRUE, thousands will be separated by a comma.
##
## The representation never uses scientific notation.
formatNum <- function(x, digits = NULL, fixed = FALSE, padtrailing = FALSE,
                                                            thousands = TRUE) {
    bigmark <- if(thousands) "," else ""
    if(!is.null(digits)) {
        x <- round(x, digits)
        if(fixed && digits > 0) {
            return(formatC(x, digits = digits, format = "f",
                big.mark = bigmark))
        }
    }
    if(padtrailing)
        return(format(x, trim = TRUE, scientific = FALSE, big.mark = bigmark))

    prettyNum(x, big.mark = bigmark, scientific = FALSE)
}

## Formatting function that can be passed to 'labels' in a scale function.
numberFormat <- function(digits = NULL, fixed = FALSE, padtrailing = FALSE,
                                                            thousands = TRUE) {
    function(x) {
        formatNum(x, digits = digits, fixed = fixed, padtrailing = padtrailing,
            thousands = thousands)
    }
}



## Returns a function that splits a given range into equally-spaced intervals
## of the specified size. The output can be passed to the breaks arg of a scale
## function.
intervalBreaks <- function(interval = 1) {
    function(rng) fullseq(rng, size = interval)
}


## Custom scales for displaying proportions as percentages.
pctAxisArgs <- function(fullaxis = TRUE, interval = 0.2) {
    list(
        breaks = if(!is.null(interval)) intervalBreaks(interval) else waiver(),
        labels = percent_format(),
        limits = if(fullaxis) c(0, 1) else NULL
    )
}

xPct <- function(fullaxis = TRUE, interval = 0.2) {
    do.call(scale_x_continuous, pctAxisArgs(fullaxis, interval))
}

yPct <- function(fullaxis = TRUE, interval = 0.2) {
    do.call(scale_y_continuous, pctAxisArgs(fullaxis, interval))
}


## Custom scales for displaying a numeric variable representing the number of
## days since a reference date.
daysAxisArgs <- function(dayticks = FALSE) {
    list(
        breaks = intervalBreaks(7),
        minor_breaks = if(dayticks) intervalBreaks(1) else NULL,
        labels = numberFormat()
    )
}

xDays <- function(dayticks = FALSE) {
    do.call(scale_x_continuous, daysAxisArgs(dayticks))
}

yDays <- function(dayticks = FALSE) {
    do.call(scale_y_continuous, daysAxisArgs(dayticks))
}


## Interval breaks on the log scale.
## Interval is in log units (eg. interval of 1 means breaks 1, 10, 100, ... for
## log base 10).
##
## Returns a function that can be used to compute both major and minor breaks.
## If computing minor breaks, these will show up at the half-way point (on the
## original scale) between the major breaks (ie. 50 between 10 and 100).
logBreaks <- function(interval = 1, minor = FALSE, base = 10) {
    function(rng) {
        breakfun <- intervalBreaks(interval)
        logbreaks <- breakfun(log(rng, base = base))
        origbreaks <- base ^ logbreaks
        if(minor) {
            ## Return the midpoints of the break intervals on the original
            ## scale.
            origbreaks <- origbreaks / 2
        }
        origbreaks
    }
}

## Format the axis for when using the log scale.
logAxisArgs <- function(largenum = FALSE, minorbreaks = TRUE, interval = 1,
                                                                    base = 10) {
    list(
        breaks = logBreaks(interval, base = base),
        minor_breaks = if(minorbreaks) {
                logBreaks(interval, minor = TRUE, base = base)
            } else { NULL },
        labels = if(largenum) largeNumLabels() else numberFormat()
    )
}

xLog10 <- function(largenum = FALSE, minorbreaks = TRUE, interval = 1) {
    do.call(scale_x_log10,
        logAxisArgs(largenum, minorbreaks, interval, base = 10))
}

yLog10 <- function(largenum = FALSE, minorbreaks = TRUE, interval = 1) {
    do.call(scale_y_log10,
        logAxisArgs(largenum, minorbreaks, interval, base = 10))
}


## Returns a function that formats large numbers in a concise and readable way.
## Numbers of magnitude larger than 1000 are displayed together with a unit
## indicating the nearest power of 1000, rounded to the specified number of
## decimal places.
## Numbers of magnitude between 1 and 1000 are rounded and displayed.
## Numbers of magnitude less than 1 are rounded, treating 'decimals' as the
## number of sig figs.
## Trailing zeros are added in the first and second cases to give all numbers
## the same number of digits after the decimal as the number with the most.
## Note that this computation is done separately for first and second cases.
largeNumLabels <- function(decimals = 2, siunits = FALSE) {
    unitlabs <- if(siunits) {
        c("K", "M", "G", "T", "P", "E")
    } else {
        c("K", "M", "B", "T")
    }
    formatLargeNum <- function(x, thousandgp) {
        numforunit <- x / 10 ^ (3 * thousandgp)
        fmttednum <- formatNum(numforunit, digits = decimals,
            padtrailing = TRUE)
        paste(fmttednum, unitlabs[thousandgp])
    }
    function(x) {
        ## Find the closest power of 10 to the number which is between it and 0.
        ## This will be -Inf when x is 0.
        precision <- trunc(log10(abs(x)))
        ## How many commas would this number have if written out fully,
        ## bounded to the largest unit label.
        thousandgp <- pmin(trunc(precision / 3), length(unitlabs))
        ## Use different formatting, depending on the magnitude of the number.
        xfmt <- rep(NA, length(x))
        largenum <- !is.na(x) & thousandgp >= 1
        mediumnum <- !is.na(x) & !largenum & abs(x) >= 1
        smallnum <- !is.na(x) & !largenum & !mediumnum
        ## For large numbers, round and tag on the unit label.
        xfmt[largenum] <- formatLargeNum(x[largenum], thousandgp[largenum])
        ## For numbers >= 1, round to the specified number of decimal places,
        ## padding as needed.
        xfmt[mediumnum] <- formatNum(x[mediumnum], digits = decimals,
            padtrailing = TRUE)
        ## For fractions, use 'decimals' as the number of sig figs.
        xfmt[smallnum] <- formatC(x[smallnum], format = "fg", digits = decimals)
        xfmt
    }
}

## Testing:
#a <- c(
#    # 0
#    0,
#    # 1
#    1,
#    # 23.4
#    23.3580,
#    # 1.2 K
#    1234,
#    # 9.0 K
#    9000,
#    # 2.8 M
#    2786452,
#    # 452.8 B
#    452782971000,
#    # 34,798.9 T
#    34798913781399587,
#    # -315.5
#    -315.51,
#    # -4.0 M
#    -3951063.61,
#    # 0.04
#    0.03536,
#    # 0.0003
#    0.0003,
#    # -0.000004
#    -0.0000041351
#)
#largeNumLabels(1)(a)


## A customized theme optimized for readability within an Rmarkdown report.
## It is based on ggplot2's theme_light and makes the following adjustments:
## - slightly larger overall text size
## - title centered and relatively smaller text
## - axis and facet label text relatively larger
## - legend on the bottom
##
## To set the theme, call theme_set(theme_dzplot()).
theme_dzplot <- function() {
    theme_light(base_size = 12) + theme(
        plot.title = element_text(hjust = 0.5, size = rel(1.1)),
        #plot.subtitle = element_text(hjust = 0.5),
        axis.text = element_text(size = rel(0.9)),
        strip.text = element_text(size = rel(0.9)),
        legend.position = "bottom"
    )
}


## From http://www.cookbook-r.com/Graphs/Multiple_graphs_on_one_page_%28ggplot2%29/
multiplot <- function(..., plotlist = NULL, cols = 1, layout = NULL, 
                                            widths = NULL, heights = NULL) {
    require(grid)
    # Make a list from the ... arguments and plotlist
    plots <- c(list(...), plotlist)
    numPlots <- length(plots)

    # If layout is NULL, then use 'cols' to determine layout
    if (is.null(layout)) {
        # Make the panel
        # ncol: Number of columns of plots
        # nrow: Number of rows needed, calculated from # of cols
        layout <- matrix(seq(1, cols * ceiling(numPlots/cols)),
            ncol = cols, nrow = ceiling(numPlots/cols))
    }

    if (numPlots == 1) {
        print(plots[[1]])
    } else {
        # Set up the page
        grid.newpage()
        layout.args <- list(nrow = nrow(layout), ncol = ncol(layout))
        if(!is.null(widths)) 
            layout.args[["widths"]] <- widths
        if(!is.null(heights))
            layout.args[["heights"]] <- heights
        
        pushViewport(viewport(layout = do.call(grid.layout, layout.args)))
        # Make each plot, in the correct location
        for (i in 1:numPlots) {
            # Get the i,j matrix positions of the regions that contain this subplot
            matchidx <- as.data.frame(which(layout == i, arr.ind = TRUE))
            print(plots[[i]], vp = viewport(layout.pos.row = matchidx$row,
                                            layout.pos.col = matchidx$col))
        }
    }
}

## Save plot both as pdf and png. 
## Pass in the filename (no extension) 
## and optionally a new width (in inches) to use, 
## keeping the same aspect ratio. 
save.both <- function(fname, adj.width = NULL, ...) {
    a <- list(...)
    if(!is.null(adj.width)) {
        a[["scale"]] <- adj.width / dev.size()[1]
    }
    for(n in c("pdf", "png")) {
        do.call(ggsave, c(filename = sprintf("%s.%s", fname, n), a))
    }
}




## Axis format for representing number of days.
#axis.days <- function(dayticks = FALSE, axis = "x") {
#    axis_fn <- get(sprintf("scale_%s_continuous", axis))
#    axis_fn(breaks = interval.breaks(7),
#        minor_breaks = if(dayticks) interval.breaks(1) else NULL)
#}
#
#xDays <- function(dayticks = FALSE) {
#    axis.days(dayticks = dayticks, axis = "x")
#}
#

## Axis format for representing percentages.
#axis.pct <- function(fullaxis = TRUE, axis = "y", interval = 0.2) {
#    axis_fn <- get(sprintf("scale_%s_continuous", axis))
#    axis_fn(breaks = interval.breaks(interval),
#        limits = if(fullaxis) c(0, 1) else NULL,
#        labels = pct.labels)
#}

#yPct <- function(fullaxis = TRUE, interval = 0.2) {
#    axis.pct(fullaxis = fullaxis, axis = "y", interval = interval)
#}
#
#xPct <- function(fullaxis = TRUE) {
#    axis.pct(fullaxis = fullaxis, axis = "x")
#}
#

## Nice labels for large numbers.
## Apply suffix for thousands/millons/billions/trillions.
## Specify how many decimals should be kept past large breaks.
###
### not in scales, but could be rewritten to include some of those
### functions
#largenum.labels <- function(decimals = 2) {
#    eval(bquote(function(n) {
#        n <- as.numeric(n)
#        ## Upper bound on abbreviations is trillions.
#        om <- pmin(trunc(trunc(log10(n)) / 3) + 1, 5)
#        ifelse(is.infinite(om), 0, sprintf("%s %s",
#            longnum(round(n / 10^(3*(om-1)), .(dec))),
#            c("", "K", "M", "B", "T")[om]))
#    }, list(dec = decimals)))
#}


#log10.breaks <- function(interval = 1, minor = FALSE) {
#    intbr <- interval.breaks(interval)
#    if(minor) {
#        function(lims) {
#            ## Add an extra interval's worth to the right endpoint on the
#            ## log scale.
#            loglims <- log10(lims) + c(0, 1)
#            ## Find the half-way point on the log scale.
#            logbreaks <- intbr(loglims) - log10(2)
#            ## Return to the original scale and check for inclusion between
#            ## the limits.
#            breaks <- 10^logbreaks
#            breaks[breaks >= lims[1] & breaks <= lims[2]]
#        }
#    } else {
#        function(lims) {
#            10^intbr(log10(lims))
#        }
#    }
#}

## Format the axis for when using the log scale.
#axis.log10 <- function(largenumlabs = FALSE, minorbreaks = TRUE, axis = "y") {
#    axis_fn <- get(sprintf("scale_%s_log10", axis))
#    interval <- 1
#    axis_fn(breaks = log10.breaks(interval),
#        minor_breaks = if(minorbreaks) log10.breaks(interval, minor = TRUE)
#            else NULL,
#        labels = if(largenumlabs) largenum.labels() else bigNum)
#}


## Update the aesthetic default values of a Geom or Stat with the given named
## argument values.
updateDefaultAes <- function(ggproto_obj, ...) {
    ggproto_obj <- substitute(ggproto_obj)
    if(!is.character(ggproto_obj)) ggproto_obj <- deparse(ggproto_obj)
    ggproto_obj <- get(ggproto_obj)
    updateAes(ggproto_obj$default_aes, ...)
}

updateAes <- function(originalaes, ..., newaes = NULL, returnlist = FALSE) {
    originalaes <- unclass(originalaes)
    updates <- if(!is.null(newaes)) {
        unclass(newaes)
    } else {
        eval(substitute(alist(...)))
    }
    updatedaes <- modifyList(originalaes, updates)
    if(!returnlist) updatedaes <- do.call(aes, updatedaes)
    updatedaes
}

## geom_col for stacked bars with the stacking order reversed.
### TODO: is this still needed?
geomCol <- function() {
    geom_col(position = position_stack(reverse = TRUE))
}


## Add a confidence band to a plot, computed using confEnvelope().
#confBand <- function(confdata = NULL) {
#    geom_ribbon(aes(ymin = lower, ymax = upper), fill = "grey70", alpha = 0.3,
#        data = confdata)
#}

## Add a confidence band to a plot, computed using confEnvelope().
## Sets default coloring and remaps ymin/ymax to "lower"/"upper".
GeomConfBand <- ggproto("GeomConfBand", GeomRibbon,
    default_aes = updateDefaultAes(GeomRibbon, fill = "grey70", alpha = 0.3))

confBand <- function(mapping = NULL, data = NULL, ..., na.rm = FALSE,
                                        show.legend = NA, inherit.aes = TRUE) {
    ## Back-compatibility:
    ## allow for the confidence envelope data to be passed as "confdata".
    dots <- list(...)
    if(!is.null(dots[["confdata"]])) {
        if(is.null(data)) data <- dots[["confdata"]]
        dots[["confdata"]] <- NULL
    }
    ## Use lower and upper as the default series for ymin and ymax.
    mapping <- updateAes(aes(ymin = lower, ymax = upper), newaes = mapping)
    layer(
        stat = "identity", geom = GeomConfBand, data = data, mapping = mapping,
        position = "identity", show.legend = show.legend,
        inherit.aes = inherit.aes, params = c(list(na.rm = na.rm), dots)
    )
}


## Function to be passed as the 'data' arg to a layer call.
## If a proportion is given, sample that proportion of the data rows.
sampledDataFun <- function(sampleprop = NULL) {
    if(!is.null(sampleprop)) {
        function(data) data[runif(nrow(data)) < sampleprop,]
    } else {
        function(data) data
    }
}

## Scatterplot points with some transparency to handle overlaps.
GeomScatterPoint <- ggproto("GeomScatterPoint", GeomPoint,
    default_aes = updateDefaultAes(GeomPoint, size = 0.2, alpha = 0.4))

scatterPoints <- function(mapping = NULL, data = NULL, jitter = FALSE,
                    sampleprop = NULL, ..., na.rm = FALSE, show.legend = NA,
                                                        inherit.aes = TRUE) {
    layer(
        geom = GeomScatterPoint, stat = "identity",
        data = sampledDataFun(sampleprop), mapping = mapping,
        position = if(jitter) "jitter" else "identity",
        show.legend = show.legend, inherit.aes = inherit.aes,
        params = list(na.rm = na.rm, ...)
    )
}

#scatterPoints <- function(jitter = FALSE, sample = NULL) {
#    data_fun <- if(!is.null(sample))
#        function(DT) {
#            DT[sample(1:.N, .N * sample)]
#        }
#    else function(DT) { DT }
#    geom_point(data = data_fun, size = 0.2, alpha = 0.4,
#        position = if(jitter) "jitter" else "identity")
#}


## Density contours to be overlaid on a scatterplot.
GeomScatterContour <- ggproto("GeomScatterContour", GeomDensity2d,
    default_aes = updateDefaultAes(GeomDensity2d, alpha = 0.7,
        colour = "darkcyan"))

scatterDensity <- function(mapping = NULL, data = NULL, sampleprop = NULL,
                    ..., na.rm = FALSE, show.legend = NA, inherit.aes = TRUE) {
    layer(
        geom = GeomScatterContour, stat = "density2d",
        data = sampledDataFun(sampleprop), mapping = mapping,
        position = "identity", show.legend = show.legend,
        inherit.aes = inherit.aes, params = list(na.rm = na.rm, ...)
    )
}

#scatterDensity <- function(sample = NULL) {
#    data_fun <- if(!is.null(sample))
#        function(DT) {
#            DT[sample(1:.N, .N * sample)]
#        }
#    else function(DT) { DT }
#    geom_density2d(data = data_fun, alpha = 0.7, colour = "darkcyan")
#}


## A geom displaying values as small red bars, by default.
GeomBoxMeans <- ggproto("GeomBoxMeans", GeomCrossbar,
    default_aes = updateDefaultAes(GeomCrossbar, colour = "darkred"))

## Add indicators for means to boxplots of distributions.
## The indicators show up as a red bar. They are actually drawn as a crossbar
## squished down to zero height.
##
## width and fatten args control the width and thickness of the bar.
boxMeans <- function(mapping = NULL, data = NULL, width = 0.2, fatten = 2.5,
                    ..., na.rm = FALSE, show.legend = NA, inherit.aes = TRUE) {
    params <- list(na.rm = na.rm, fun.y = mean)
    ## Add in ymin and ymax for the crossbar geom, setting them equal to get
    ## zero height.
    params[["fun.ymin"]] <- params[["fun.y"]]
    params[["fun.ymax"]] <- params[["fun.y"]]
    params <- modifyList(params, list(...), keep.null = TRUE)
    layer(
        geom = GeomBoxMeans, stat = "summary", data = data, mapping = mapping, 
        position = "identity", inherit.aes = inherit.aes,
        show.legend = show.legend, params = params
    )
}

#boxMeans <- function(size = 10, horiz = FALSE) {
#    stat_summary(fun.y = mean, shape = if(horiz) 124 else 95, geom = "point",
#        size = size, colour = "darkred")
#}

## Add indicators for means to boxplots of distributions.
## The indicator show up as a red bar.
#GeomBoxMeans <- ggproto("GeomBoxMeans", GeomPoint,
#    default_aes = updateDefaultAes(GeomPoint, shape = 95, colour = "darkred",
#        size = 10))
#boxMeans <- function(mapping = NULL, data = NULL, horiz = FALSE, ...,
#                        na.rm = FALSE, show.legend = NA, inherit.aes = TRUE) {
#    params <- list(na.rm = na.rm, fun.y = mean)
#    if(horiz) params[["shape"]] <- 124
#    params <- modifyList(params, list(...), keep.null = TRUE)
#    layer(
#        geom = GeomBoxMeans, stat = "summary", data = data, mapping = mapping, 
#        position = "identity", inherit.aes = inherit.aes,
#        show.legend = show.legend, params = params
#    )
#}


#----------------------------------------------------------------------------
#
# Deprecated functions (already available in scales package)
#


##--- Deprecated ---
## Generate regular tick intervals.
## Specify the interval width.
###
### scales:::fullseq handles this.
### defaults to covering endpoints of the range, but this doesn't affect
### the limits displayed in a plot (ie. the range isn't inflated because of
### this).
interval.breaks <- function(interval = 1) {
    if(identical(interval, 1)) {
        function(lims) { ceiling(lims[1]) : floor(lims[2]) }
    } else {
        eval(bquote(function(lims) {
            seq((ceiling(lims[1] / .(int)) * .(int)),
                (floor(lims[2] / .(int)) * .(int)),
                by = .(int))
        }, list(int = interval)))
    }
}

##--- Deprecated ---
## Apply formatting string to dates.
###
### covered by the date_labels arg passed to scale_x_date
date.labels <- function(fmt = "%Y-%m-%d") {
    eval(bquote(function(d) {
        format(d, .(fmt))
    }, list(fmt = fmt)))
}


##--- Deprecated ---
## Format proportion as a percentage for labelling.
### scales::percent_format, scales::percent
pct.labels <- function(n) {
    n <- as.numeric(n) * 100
    sprintf("%s%%", n)
}

##--- Deprecated ---
## Rewritten and renamed above.
largenum.labels <- function(decimals = 2) {
    largeNumLabels(decimals)
}

