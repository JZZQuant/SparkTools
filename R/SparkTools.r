# A Stack object backed by a list. The backing list will grow or shrink as
# the stack changes in size.
library(R6)

Quickframe <- R6Class(
  'Quickframe',
  portable = TRUE,
  public = list
  (
    path=NA,
    table=NA,
    packages=list(),
    master="yarn-client",
    csv=TRUE,

    initialize = function(packages=list() ,master="yarn-client",csv=TRUE,path,table) {
      if(!missing(path) && ! missing(table)){
        self$path=path
        self$table=table
      }
        self$packages=packages
        self$master=master
        self$csv=csv
        if (self$csv){self$packages<-c(self$packages,"com.databricks:spark-csv_2.10:1.3.0")}
        if (length(self$packages)>0)
        {
          arguments=paste(c("--packages",self$packages,"sparkr-shell"),collapse = " ")
          Sys.setenv('SPARKR_SUBMIT_ARGS'=arguments)
        }
        self$connect(Quickframe$set)
    },

    connect = function(Force=F){
      require(SparkR)
      if(Force || Quickframe$set)
      {
        Quickframe$sc <- sparkR.init(sparkPackages=paste(self$packages,collapse=" "),master=self$master)
        Quickframe$sqlContext <- sparkRSQL.init(Quickframe$sc)
        Quickframe$set<-F
      }
      if (!is.na(self$path) && !is.na(self$table)){self$RegisterCSV(self$path,self$table)}
  },

    RegisterCSV = function(path,table){
             require(SparkR)
             private$dataobject<-read.df(Quickframe$sqlContext,path , source = "csv", delimiter=",", header="true", inferSchema="true")
             registerTempTable(private$dataobject, table)
             self$table<-table
             self$path<-path
           },

    Info=function(numeric=TRUE){
      structure<-coltypes(private$dataobject)
      features<-columns(private$dataobject)
      hausdorff<-features[structure=="numeric"]
      descrete<-features[structure=="character"]
      if (numeric){head(describe(select(private$dataobject,hausdorff)))}
      else{private$descrete_describe(select(private$dataobject,descrete))}
    },

    df =function(){private$dataobject}
  ),

  private = list
  (
    descrete_describe= function(rdd){
    registerTempTable(rdd,"temp_descrete")
    features<-columns(rdd)
    sapply(features, function(x)  {
      query<-sprintf("select mean(count) as mean_count,sum(count) as count ,min(count) as min_count,max(count) as max_count,variance(count) as variance_count,sum(entropy) as entropy from
                     (select count, 1/count as freq , ((1/count)*log(count)) as entropy from
                     (select %s,count(%s) as count from temp_descrete group by %s) as t2)as t1", x,x,x)
      head(self$Q(query))})
    },
    dataobject=NULL
  )
)

#these three still need to be branded as private variables to avoid accedental modification
Quickframe$sc<-NULL
Quickframe$sqlContext<-NULL
Quickframe$set<-T

Quickframe$Q <- function(query){
  require(SparkR)
  sql(Quickframe$sqlContext,query)}
