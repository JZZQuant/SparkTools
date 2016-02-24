# Hello, world!
#
# This is an example function named 'hello'
# which prints 'Hello, world!'.
#
# You can learn more about package authoring with RStudio at:
#
#   http://r-pkgs.had.co.nz/
#
# Some useful keyboard shortcuts for package authoring:
#
#   Build and Reload Package:  'Ctrl + Shift + B'
#   Check Package:             'Ctrl + Shift + E'
#   Test Package:              'Ctrl + Shift + T'
library(rJava)

SparkRHelper<-SparkRHelper <- setClass("SparkRHelper",slots=c(sqlContext = "jobj",sc   = "jobj"))
#constructor
setMethod ("initialize",
           "SparkRHelper",
           function(.Object,packages=list() ,master="yarn-client",csv=TRUE){
             if (csv){packages<-c(packages,"com.databricks:spark-csv_2.10:1.3.0")}
             if (length(packages)>0)
             {
               arguments=paste(c("--packages",packages,"sparkr-shell"),collapse = " ")
               Sys.setenv('SPARKR_SUBMIT_ARGS'=arguments)
             }
             library(SparkR)
             .Object@sc <- sparkR.init(sparkPackages=paste(packages,collapse=" "),master=master)
             .Object@sqlContext <- sparkRSQL.init(.Object@sc)
             return(.Object)
           }
)

setGeneric("RegisterCSV", function(helper,path,table) standardGeneric("RegisterCSV"))
setGeneric("Q", function(helper,query) standardGeneric("Q"))

#create a new data frame from a csv
setMethod ("RegisterCSV",signature(helper="SparkRHelper", path="character",table="character"),
           function(helper,path="data/train.csv",table){
             require(SparkR)
             df<-read.df(helper@sqlContext,path , source = "csv", delimiter=",", header="true", inferSchema="true")
             registerTempTable(df, table)
           }
)

#create a new data frame from a csv
setMethod ("Q",signature(helper="SparkRHelper", query="character"),
           function(helper,query){
             require(SparkR)
             sql(helper@sqlContext,query)
             }
)


