#######################################################################
###  
###  GGPlot customizations.
###  
#######################################################################


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


## Generate regular tick intervals.
## Specify the interval width. 
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


## Nice labels for large numbers. 
## Apply suffix for thousands/millons/billions/trillions.
## Specify how many decimals should be kept past large breaks.
largenum.labels <- function(decimals = 2) { 
    eval(bquote(function(n) {
        n <- as.numeric(n)
        ## Upper bound on abbreviations is trillions.
        om <- pmin(trunc(trunc(log10(n)) / 3) + 1, 5)
        ifelse(is.infinite(om), 0, sprintf("%s %s", 
            longnum(round(n / 10^(3*(om-1)), .(dec))), 
            c("", "K", "M", "B", "T")[om]))
    }, list(dec = decimals)))
}


## Apply formatting string to dates. 
date.labels <- function(fmt = "%Y-%m-%d") {
    eval(bquote(function(d) {
        format(d, .(fmt))
    }, list(fmt = fmt)))
}


## Format proportion as a percentage for labelling.
pct.labels <- function(n) {
    n <- as.numeric(n) * 100
    sprintf("%s%%", n)
}


## Axis format for representing number of days.
axis.days <- function(dayticks = FALSE, axis = "x") {
    axis_fn <- get(sprintf("scale_%s_continuous", axis))
    axis_fn(breaks = interval.breaks(7),
        minor_breaks = if(dayticks) interval.breaks(1) else NULL)
}

xDays <- function(dayticks = FALSE) {
    axis.days(dayticks = dayticks, axis = "x")
}


## Axis format for representing percentages.
axis.pct <- function(fullaxis = TRUE, axis = "y", interval = 0.2) {
    axis_fn <- get(sprintf("scale_%s_continuous", axis))
    axis_fn(breaks = interval.breaks(interval),
        limits = if(fullaxis) c(0, 1) else NULL,
        labels = pct.labels)
}

yPct <- function(fullaxis = TRUE, interval = 0.2) {
    axis.pct(fullaxis = fullaxis, axis = "y", interval = interval)
}

xPct <- function(fullaxis = TRUE) {
    axis.pct(fullaxis = fullaxis, axis = "x")
}


## Use interval breaks on the log10 scale.
## Interval is in log10 units (eg. interval of 1 means breaks 1, 10, 100, ...)
## If computing minor breaks, these will show up at the half-way point (on the
## original scale) between the major breaks (ie. 50 between 10 and 100).
log10.breaks <- function(interval = 1, minor = FALSE) {
    intbr <- interval.breaks(interval)
    if(minor) {
        function(lims) {
            ## Add an extra interval's worth to the right endpoint on the
            ## log scale.
            loglims <- log10(lims) + c(0, 1)
            ## Find the half-way point on the log scale.
            logbreaks <- intbr(loglims) - log10(2)
            ## Return to the original scale and check for inclusion between
            ## the limits.
            breaks <- 10^logbreaks
            breaks[breaks >= lims[1] & breaks <= lims[2]]
        }
    } else {
        function(lims) {
            10^intbr(log10(lims))
        }
    }
}

## Fix for required long number formatting function.
longnum <- bigNum
## Format the axis for when using the log scale.
axis.log10 <- function(largenumlabs = FALSE, minorbreaks = TRUE, axis = "y") {
    axis_fn <- get(sprintf("scale_%s_log10", axis))
    interval <- 1
    axis_fn(breaks = log10.breaks(interval),
        minor_breaks = if(minorbreaks) log10.breaks(interval, minor = TRUE)
            else NULL,
        labels = if(largenumlabs) largenum.labels() else bigNum)
}

yLog <- function(largenumlabs = FALSE, minorbreaks = TRUE) {
    axis.log10(largenumlabs = largenumlabs, minorbreaks = minorbreaks,
        axis = "y")
}

xLog <- function(largenumlabs = FALSE, minorbreaks = TRUE) {
    axis.log10(largenumlabs = largenumlabs, minorbreaks = minorbreaks,
        axis = "x")
}


## geom_col for stacked bars with the stacking order reversed.
geomCol <- function() {
    geom_col(position = position_stack(reverse = TRUE))
}


## Add a confidence band to a plot, computed using confEnvelope().
confBand <- function(confdata = NULL) {
    geom_ribbon(aes(ymin = lower, ymax = upper), fill = "grey70", alpha = 0.3,
        data = confdata)
}


## Scatterplot points with some transparency to handle overlaps.
scatterPoints <- function(jitter = FALSE, sample = NULL) {
    data_fun <- if(!is.null(sample))
        function(DT) {
            DT[sample(1:.N, .N * sample)]
        }
    else function(DT) { DT }
    geom_point(data = data_fun, size = 0.2, alpha = 0.4,
        position = if(jitter) "jitter" else "identity")
}


## Density contours to be overlaid on a scatterplot.
scatterDensity <- function(sample = NULL) {
    data_fun <- if(!is.null(sample))
        function(DT) {
            DT[sample(1:.N, .N * sample)]
        }
    else function(DT) { DT }
    geom_density2d(data = data_fun, alpha = 0.7, colour = "darkcyan")
}


## Add indicators for means to boxplots of distributions.
## The indicator show up as a red bar.
boxMeans <- function(size = 10, horiz = FALSE) {
    stat_summary(fun.y = mean, shape = if(horiz) 124 else 95, geom = "point",
        size = size, colour = "darkred")
}

