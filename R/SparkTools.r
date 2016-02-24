# A Stack object backed by a list. The backing list will grow or shrink as
# the stack changes in size.
library(R6)

SparkRHelper <- R6Class(
  'SparkRHelper',
  portable = TRUE,
  public = list(
    sc=NULL,
    sqlContext=NULL,
    initialize = function(packages=list() ,master="yarn-client",csv=TRUE) {
             if (csv){packages<-c(packages,"com.databricks:spark-csv_2.10:1.3.0")}
             if (length(packages)>0)
             {
               arguments=paste(c("--packages",packages,"sparkr-shell"),collapse = " ")
               Sys.setenv('SPARKR_SUBMIT_ARGS'=arguments)
             }
             library(SparkR)
             self$sc <- sparkR.init(sparkPackages=paste(packages,collapse=" "),master=master)
             self$sqlContext <- sparkRSQL.init(self$sc)
    },

    RegisterCSV = function(path="data/train.csv",table){
             require(SparkR)
             df<-read.df(self$sqlContext,path , source = "csv", delimiter=",", header="true", inferSchema="true")
             registerTempTable(df, table)
           },

    Q = function(query){
             require(SparkR)
             sql(self$sqlContext,query)
             }
  ),

  private = list(

  )
)
