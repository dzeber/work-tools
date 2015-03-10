#######################################################################
###  
###  GGPlot customizations.
###  
#######################################################################


## From http://www.cookbook-r.com/Graphs/Multiple_graphs_on_one_page_%28ggplot2%29/
multiplot <- function(..., plotlist = NULL, cols = 1, layout = NULL, widths = NULL, heights = NULL) {
  require(grid)

  # Make a list from the ... arguments and plotlist
  plots <- c(list(...), plotlist)

  numPlots = length(plots)

  # If layout is NULL, then use 'cols' to determine layout
  if (is.null(layout)) {
    # Make the panel
    # ncol: Number of columns of plots
    # nrow: Number of rows needed, calculated from # of cols
    layout <- matrix(seq(1, cols * ceiling(numPlots/cols)),
                    ncol = cols, nrow = ceiling(numPlots/cols))
  }

 if (numPlots==1) {
    print(plots[[1]])

  } else {
    # Set up the page
    grid.newpage()
    layout.args = list(nrow = nrow(layout), ncol = ncol(layout))
    if(!is.null(widths))
        layout.args[["widths"]] = widths
    if(!is.null(heights))
        layout.args[["heights"]] = heights
        
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
