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

hello <- function() {
  Sys.setenv('SPARKR_SUBMIT_ARGS'='"--packages" "com.databricks:spark-csv_2.10:1.3.0" "sparkr-shell"')
  library(SparkR)
  sc <- sparkR.init(sparkPackages="com.databricks:spark-csv_2.10:1.3.0",master="yarn-client")
  sqlContext <- sparkRSQL.init(sc)
  train<-read.df(sqlContext, "data/train.csv", source = "csv", delimiter=",", header="true", inferSchema="true")
}
