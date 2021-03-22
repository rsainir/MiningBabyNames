#Read the name of the input file and output file from the command line.
#You can access the command line arguments via the string array ARGS. See this post
inputName = ARGS[1]
outputName = ARGS[2]
#print(input)

using ZipFile
r = ZipFile.Reader(inputName) #read zipfile, put in r
# for f in r.files
#     println("Filename: $(f.name)")
#     write(stdout, read(f, String));
# end

using SQLite, DBInterface
 #create new db with output name
db= SQLite.DB(outputName)
SQLite.execute(db, "CREATE TABLE IF NOT EXISTS names (
year INTEGER,
name TEXT,
sex TEXT,
num INTEGER
);")

#SQLite.tables(db) #display tables

#scan zip, for file w name yob????.txt, scan with csv

stmt=DBInterface.prepare(db, "INSERT INTO names VALUES(?,?,?,?);")
for f in r.files
    SQLite.transaction(db) #start transaction
    if f.name[1:3] == "yob"
        year = f.name[4:7]
        using CSV, DataFrames
        #println(f.name)
        c = CSV.read(f, DataFrame, header=false) #scan as csv file //header=false or datarow=1?
        #check to see if header=false is necessary
        for i in 1:size(c,1)
            name = c[i,"Column1"]
            sex = c[i,"Column2"]
            num = c[i,"Column3"]
            DBInterface.execute(stmt,[year,name,sex,num])
        end
        SQLite.commit(db) #commit for whole file
    end
end
close(r)
#SQLite auto closes databases
#print(SQLite.tables(db)) #display tables
