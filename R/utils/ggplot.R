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



