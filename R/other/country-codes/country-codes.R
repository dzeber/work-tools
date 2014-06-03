#######################################################################
###  
###  Generate mappings between two-letter MaxMind country codes
###  and sensible full country names useful for labelling graphics.
###  Also includes functionality for continents/regions. 
###  
#######################################################################


require(countrycode)
require(data.table)
require(rjson)

## URLs from which to download MaxMind tables.
table.urls = c(names = "http://dev.maxmind.com/static/csv/codes/iso3166.csv",
    continents = "http://dev.maxmind.com/static/csv/codes/country_continent.csv")
    
## Missing values to fill in for MaxMind tables.
mm.missing.cont = setNames(c("NA", "AF", "SA", "NA"), 
    c("SX", "SS", "CW", "BQ"))
mm.missing.nm = setNames(c("France Met", "Dutch Antilles"), c("FX", "AN"))

## Continent code mappings. 
cont.nms = c("NA" = "North America",
    "SA" = "South America",
    "EU" = "Europe",
    "AF" = "Africa",
    "AS" = "Asia", 
    "OC" = "Oceania",
    "AN" = "Antarctica")    

## Missing regions.   
missing.reg = c("AP" = "--", 
    "EU" = "--",
    "FX" = "Western Europe",
    "TK" = "Polynesia",
    "UM" = "--",
    "TW" = "Eastern Asia",
    "NA" = "Southern Africa",
    "IO" = "Southern Asia",
    "CC" = "Australia and New Zealand",
    "CX" = "Australia and New Zealand",
    "BQ" = "Caribbean")
    
## Manual name replacements. 
repl.nms = c(AE = "UAE",
    BN = "Brunei",
    CC = "Cocos Islands",
    GB = "UK",
    KP = "North Korea",
    KR = "South Korea",
    LA = "Laos", 
    LY = "Libya", 
    MP = "Mariana Islands",
    PS = "Palestine",
    RU = "Russia", 
    SY = "Syria", 
    TC = "Turks and Caicos",
    US = "US", 
    UM = "US Minor Outlying Islands",
    VA = "Vatican",
    VG = "British Virgin Islands", 
    VI = "US Virgin Islands")

## Download country name and continent tables from MaxMind. 

countries = mapply(function(...) { as.data.table(read.csv(...)) }, 
    table.urls, 
    col.names = list(c("code", "name"), c("code", "continent")), 
    header = c(FALSE, TRUE),
    MoreArgs = list(na.strings = "", stringsAsFactors = FALSE),
    SIMPLIFY = FALSE)
    
## There shouldn't be any NAs in these tables...
if(any(unlist(lapply(countries, is.na))))
    stop("NAs were found in the tables")

## Join tables.
    
for(ct in countries) setkey(ct, "code")
countries = merge(countries$names, countries$continents, all = TRUE)

## Fill in missing continents by hand. 
## Seems data.table will only do replacements one by one...
for(cc in names(mm.missing.cont))
    countries[cc, continent := mm.missing.cont[cc]]
for(cc in names(mm.missing.nm))
    countries[cc, name := mm.missing.nm[cc]]

## Assert no NAs remaining. 
if(any(unlist(lapply(countries, is.na))))
    stop("NAs were still found in the tables")

## Substitute continent names. 
countries[continent %in% names(cont.nms), continent := cont.nms[continent]]

## At this point we have complete codes mapping to country name and continent. 
## Look up regions from countrycode package. 

## Join to countrycodes_data. 
ccd = as.data.table(countrycode_data)
ccd = ccd[!is.na(iso2c), list(code = iso2c, region = region)]
setkey(ccd, code)

countries = merge(countries, ccd, all = TRUE)

## Fill in missing regions. 
countries[continent == "--", region := "--"]
countries[continent == "Antarctica", region := "Antarctica"]
for(cc in names(missing.reg))
    countries[cc, region := missing.reg[cc]]

## Assert no NAs remaining. 
if(any(unlist(lapply(countries, is.na))))
    stop("NAs were still found in the tables")

## Format (shorten) country names. 

## Manual replacements. 
for(cc in names(repl.nms))
    countries[cc, name := repl.nms[cc]]

## Remove extra denominations. 
countries[, name := sub("(\\s\\(.+\\)|,.+of.*$)", "", name)]
## Contract compound names. 
countries[, name := gsub(",\\s", "/", sub("\\sand\\s(the\\s)?", "/", name))]

nms.vec = setNames(countries[, name], countries[, code])
cont.vec = setNames(countries[, continent], countries[, code])
reg.vec = setNames(countries[, region], countries[, code])


## Create function to look up country info from 2-letter GeoIP country codes. 
## Input to the function will be a character vector of 2-letter country codes, 
## and a way to handle codes that were not found in the list.
## Possible values are:
## - "remove", to remove them from the output (NULL will do this too)
## - "unchanged", to return them unchanged, or
## - a string to represent the missing values 
##  (setting this to NA will use NAs for the missing codes).
## Default is to use the string "--". 
##
## Input to the generetor function is a lookup dictionary to use 
## (a named vector mapping country codes to info),
## and whether or not to check for "--" values in the output, 
## which indicate "unknown". 
## ** maybe don't need two separate cases. **

create.lookup <- function(dict
                            # , check.vals = FALSE
                                                    ) {
    # na.vals <- if(check.vals) {
        # quote(is.na(nms) | nms == "--")
    # } else { 
        # quote(is.na(nms)) 
    # }
    f <- 
        # eval(bquote(
        function(codes, unknown.code = "--") {
        if(missing(codes) || !is.character(codes))
            stop("Country codes must be specified as a character vector")
            
        if(!missing(unknown.code)) {
            if(!isTRUE(is.null(unknown.code)) && !isTRUE(is.na(unknown.code))) {
                ## unknown.code should have length 1.
                if(length(unknown.code) == 0)
                    stop("Argument 'unknown.code' must have length 1.")
                if(length(unknown.code) > 1) {
                    warning("Argument 'unknown.code' has length > 1. Using the first element.")
                    unknown.code <- unknown.code[1]
                }
                
                if(identical(unknown.code, "remove")) {
                    unknown.code <- NULL
                } else {
                    if(!is.character(unknown.code))
                        unknown.code <- as.character(unknown.code)
                }
            }
        }
        
        nms <- as.vector(dict[toupper(codes)])
        
        ## Replace any missing codes with appropriate identifier. 
        # na.nms <- .(navals)
        na.nms <- is.na(nms) | nms == "--"
        if(any(na.nms)) {
            if(identical(unknown.code, "unchanged")) {
                nms[na.nms] <- codes[na.nms]
            } else {
                if(is.null(unknown.code)) {
                    nms <- nms[!na.nms]
                } else {
                    nms[na.nms] <- unknown.code
                }
            }
        }
        
        nms    
    }
        # , list(navals = na.vals)))
    environment(f) = list2env(list(dict = dict), parent = globalenv())
    f
}

## Look up country names from 2-letter GeoIP country codes. 
## The names are generally compact enough to use for plot labelling. 
##
## Input a character vector of 2-letter country codes. 
##
## Also optionally specify a way to handle codes that were not found in the list.
## Possible values are:
## - "remove", to remove them from the output (NULL will do this too)
## - "unchanged", to return them unchanged, or
## - a string to represent the missing values 
##  (setting this to NA will use NAs for the missing codes).
## Default is to use the string "--". 

country.name <- create.lookup(nms.vec)

## Look up continent names corresponding to countries
## represented as 2-letter GeoIP country codes. 
##
## Supply a vector of country codes,
## and optionally the string to use to represent codes
## that were not found. 

geo.continent = create.lookup(cont.vec
                                # , check = TRUE
                                                )

## Look up world region names corresponding to countries
## represented as 2-letter GeoIP country codes. 
##
## Supply a vector of country codes,
## and optionally the string to use to represent codes
## that were not found. 

geo.region = create.lookup(reg.vec
                            # , check = TRUE
                                            )


## Save functions and table as RData. 
save(countries, country.name, geo.continent, geo.region, 
    file = "countrycodes.RData")
                                            
## Create JSON version of lookup table. 

countriesj = setNames(lapply(1:nrow(countries), function(i) {
    ll = as.list(countries[i, list(name, continent, region)])
}), countries[, code])
    
countriesj = toJSON(countriesj)
cat(countriesj, file = "countrycodes.json")



