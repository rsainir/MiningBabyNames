

dbName=ARGS[1]
inName=ARGS[2]
inSex=ARGS[3]


using SQLite, DBInterface
db=SQLite.DB(dbName)
#should return dataframe with the results of the query
using DataFrames
data = DBInterface.execute(db, "SELECT names.year, names.num
FROM names
WHERE names.name = '$inName' AND names.sex='$inSex'
")

queryRes=DataFrame(data)

sort!(queryRes,[order(:year)]) #sort by year

#print(queryRes)

using Gadfly
display(plot(queryRes, x=:year, y=:num))
