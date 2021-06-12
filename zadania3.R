library(tidyverse)
library(rlang)
library(DBI)
library(RSQLite)

konta <- read_csv('konta.csv')

readToBase<- function(filepath,dbpath,tablename,header=TRUE,size,sep=",",deleteTable=TRUE){
  ap= !deleteTable
  ov= deleteTable
  fileConnection<- file(description = filepath,open = "r")
  dbConn<-dbConnect(SQLite(),dbpath)
  data<-read.table(fileConnection,nrows=size,header = header,fill=TRUE,sep=sep)
  columnsNames<-names(data)
  dbWriteTable(conn = dbConn,name=tablename,data,append=ap,overwrite=ov)
  repeat{
    if ( nrow(data)==0){
      close(fileConnection)
      dbDisconnect(dbConn)
      break
    }
    data<-read.table(fileConnection,nrows=size,col.names=columnsNames,fill=TRUE,sep=sep)
    dbWriteTable(conn = dbConn,name=tablename,data,append=TRUE,overwrite=FALSE)
  }
}

readToBase("konta.csv", "bazaKonta.sqlite", "konta", size=1000)


# Zadanie 1
rankAccount <- function(dataFrame,colName,groupName,valueSort,num) {
  dataFrame %>% 
    filter( !!sym(colName) == sym(groupName)) %>%
    arrange(desc(!!sym(valueSort))) %>%
    head(num)
}

rankAccount(konta,"occupation","NAUCZYCIEL","saldo",10)

# Zadanie 2
rankAccountBigDatatoChunk <- function(filename, size, colName, groupName, valueSort, num) {
  fileConnection<-file(description = filename,open="r")
  joinData<-read.table(fileConnection,nrows=size,header = TRUE,fill=TRUE,sep=",")
  columnsNames<-names(joinData)
  repeat{
    sortedData <- joinData %>%
      filter( !!sym(colName) == sym(groupName)) %>%
      arrange(desc(!!sym(valueSort))) %>%
      head(num)
    data<-read.table(fileConnection,nrows=size,col.names = columnsNames,fill=TRUE,sep=",")
    if(nrow(data) == 0){
      break
    }
    joinData <- rbind(sortedData, data)
  }
  sortedData
}

rankAccountBigDatatoChunk(filename = "konta.csv", 1000,
                          "occupation", "NAUCZYCIEL", "saldo",7)

# Zadanie 3
rankAccountDatabase <- function(dbp,tablename,colName,groupName,valueSort,num) {
  con=dbConnect(SQLite(),dbp)
  print(groupName)
  dbGetQuery(con, paste0("SELECT * FROM ",tablename, " WHERE ",colName,"='",groupName,
                         "' ORDER BY ",valueSort," DESC LIMIT ",num,";" ) )
}
rankAccountDatabase("bazaKonta.sqlite", "konta","occupation","NAUCZYCIEL","saldo",10)
