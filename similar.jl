using DataFrames

function colnames(df::DataFrame)
    for name in names(uniquesname)
        print(":", name, ", ")
    end
end

# 1
using SQLite
db=SQLite.DB("names.db")
#should return dataframe with the results of the query

d = DBInterface.execute(db, "SELECT * FROM names;")
d= DataFrame(d)

#all column names
# for name in names(d)
#     print(":", name, ", ")
# end

# 2
uniquesname = unique(d, [:name,:sex]) #returns a df that only has unique names

grouped = combine(groupby(uniquesname, [:sex]), nrow=>:countnames) #group by sex, aggregate based on grouping
groupedYr = combine(groupby(uniquesname, [:year]), nrow=>:namesperyear) #group by sex, aggregate based on grouping
#display(groupedYr)
Ng = grouped[1,:countnames] #gets count of girl names
Nb = grouped[2,:countnames] #gets count of boy names
Ny = size(groupedYr, 1)

#println(Ng) 62976
#println(Nb) 36468
#println(Ny) 140

# 3
DictbNI = Dict() #given a name, we want to map to an index
DictbIN = Dict()

DictgNI = Dict()
DictgIN = Dict()

DictY = Dict()
DictYr = Dict()
# need dataframe with unique girl names
# data[data[:,1] .== 1, :]
uniquegirl = uniquesname[uniquesname[:,:sex].=="F",:]
uniqueboy = uniquesname[uniquesname[:,:sex].=="M",:]
#display(DataFrame(uniquegirl))
#year_index => year, year => year_index
for i in 1:Nb #from 1 to number of boys names, assign map from index i (integer) to name
    #need to set two different directions with two different maps
    DictbNI[i] = uniqueboy[i,:name]
    DictbIN[uniqueboy[i,:name]] = i
    #print(DictbNI[i])
end
#######################
#print(DictbIN["Johny"])

for i in 1:Ng #from 1 to number of girls names, assign map from index i (integer) to name
    #need to set two different directions with two different maps
    DictgNI[i] = uniquegirl[i,:name]
    DictgIN[uniquegirl[i,:name]] = i
end

#want all the years, mapping from 1 to 1880, 2 to 1881, etc
count = 1880; #start at 1880
for i in 1:Ny
    DictY[i] = count
    DictYr[count] = i
    global count+=1
end

# 4
#initiazlize Fb (Nb x Ny), Fg (Ng x Ny)
Fb = zeros(Nb, Ny)
Fg = zeros(Ng, Ny)

# 5
#scan d, add counts to to Fb every time a certain name appears (frequency of names)
tbl = Tables.rowtable(d)
for n in Tables.rows(tbl)
    if(n.sex == "F")
        Fg[DictgIN[n.name], DictYr[n.year]] += 1
    elseif(n.sex == "M")
        Fb[DictbIN[n.name], DictYr[n.year]] += 1 #get the index at which that name is, and at which that year is, increment the frequency of the name for that specific year in the matrix
    #DictbNI[]
    end
end

#display(Fg)

#vector of total children born in each year (size Ny)
Ty = zeros(Ny)

#6
for n in Tables.rows(tbl)
    Ty[DictYr[n.year]] += 1
end


#Pb and Pg take ratio of frequency to total number of children that year
# need frequency of name from Fb and Fg, and total number of children in each year from Ty

Pb = zeros(Nb, Ny)
Pg = zeros(Ng, Ny)
# 7
for i in 1:Nb #name index
    for j in 1:Ny #year index
        Pb[i,j] = (Fb[i,j] / Ty[j])
    end
end

for i in 1:Ng #name index
    for j in 1:Ny #year index
        Pg[i,j] = (Fg[i,j] / Ty[j])
    end
end

# tot = 0
#
# for i in 1:Nb
#     global tot += Pb[i,DictYr[1885]]
# end
#
# for i in 1:Ng
#     global tot += Pg[i,DictYr[1885]]
# end
# print(tot) ## should equal zero


# compute Qb and Qg that normalize values across years such that...
# L2 norm of all row vectors is 1 (per row, where row is an instance of a name of a kid)
using LinearAlgebra
#8
Qb = zeros(Nb,Ny)
Qg = zeros(Ng,Ny)

#l2 norm for boys
for i in 1:Nb
    Qb[i,:]=normalize(@view Pb[i,:]) #normalize row, normalize shrinks matrix, returns a vector such that norm(vector)=1
    #print(norm(Qb[i,:]))
end
#print(size(Qb))
#l2 norm for girls
for i in 1:Ng
    Qg[i,:]=normalize(@view Pg[i,:]) #normalize row, normalize shrinks matrix, returns a vector such that norm(vector)=1
    #print(norm(Qb[i,:]))
end
#Qb should have Nb rows

#split Qb and Qg into 10 fragments each
#Qb size is


#intervals for partitioning (for first 9 partitions at least)
# 9
bsd = Int(floor(size(Qb,1)/10))
bsg = Int(floor(size(Qg,1)/10))


#init partitions
QbFrags = Vector(undef, 10) #vector of size 10
QgFrags = Vector(undef, 10) #vector of size 10

#partition boys
#print(Nb)
tot = 1
for i in 1:10
    if(i==10 && size(Qb,1)-tot > (bsd)) #if were at the laast index, and have too much left, stuff it all into the final index
        nxt = size(Qb,1)
        QbFrags[i] = @view Qb[tot:nxt,:]
        global tot += (bsd) #4205 per thing
    else
        nxt = tot + (bsd-1)
        QbFrags[i] = @view Qb[tot:nxt,:]
        global tot += (bsd) #4205 per thing
    end
end

#partition girls
tot=1
for i in 1:10
    if(i==10 && size(Qg,1)-tot > (bsg)) #if were at the laast index, and have too much left, stuff it all into the final index
        nxt = size(Qg,1) #advance to the end if we cant fit
        QgFrags[i] = @view Qg[tot:nxt,:]
        global tot += (bsg) #4205 per thing
    else
        nxt = tot + (bsg-1)
        QgFrags[i] = @view Qg[tot:nxt,:]
        global tot += (bsg) #4205 per thing
    end
end

 println(size(QgFrags[1]))
# println(size(QgFrags[2]))
# println(size(QgFrags[4]))
 println(size(QgFrags[10]))
#
 println(size(QbFrags[1]))
# println(size(QbFrags[2]))
# println(size(QbFrags[4]))
 println(size(QbFrags[10]))
# println(bsg)
# println(bsd)

using BenchmarkTools
using Base.Threads

function l() #function wrapper to avoid using globals
    BLAS.set_num_threads(10)

    maxindex = CartesianIndex()
    #maxindex = Tuple{Int,Int}
    #A*B' to get array of dot products
    max = 0
    maxbpart = 0
    maxgpart = 0
    maxpartsizeg = 0
    maxpartsizeb = 0

    for b in 1:10
       @threads for g in 1:10
            dd = QbFrags[b]*copy(transpose(QgFrags[g])) #inner product
            #println(size(dd))
    #@time begin
            v = findmax(dd)
             if(v[1] > max)
                 max = v[1]
                 #print(max)
                 maxindex = v[2]
                 maxbpart = b
                 maxgpart = g
                 maxpartsizeb=size(dd,1)
                 maxpartsizeg=size(dd,2)
             end
            #end
        end
    end

    #display(maxindex)
    maxrealindexboy = ((maxbpart-1)*maxpartsizeb) + (maxindex[1]+1)
    maxrealindexgirl = ((maxgpart-1)*maxpartsizeg) + (maxindex[2]+1)


    println(maxrealindexboy)
    println(maxrealindexgirl)
    println(maxbpart)
    println(maxgpart)
    println(maxpartsizeb)
    println(maxpartsizeg)

    # 10
    println("Max cosine diff: ")
    println(max)
    println("Index for partitions ", maxbpart , ", ", maxgpart  )
    println(maxindex) # max index

    println("Boy name: ")
    println(DictbNI[maxrealindexboy])
    println("Girl name: ")
    println(DictgNI[maxrealindexgirl])

    print(dot(Qb[maxrealindexboy,:],Qg[maxrealindexgirl,:]))
end
l()
