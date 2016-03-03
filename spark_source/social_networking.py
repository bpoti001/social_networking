from pyspark import SparkContext
import sys
sc=SparkContext(appName="socialnetworking")
file = sc.textFile(sys.argv[1], 1)
def func1(line):
        output=[]
        parts=str(line).split()
        v=int(parts[0])
        nbrs=parts[1].split(',')
        nbrs=[int(i) for i in nbrs]
        for i in range(len(nbrs)-1):
                for j in range(i+1, len(nbrs)):
                        tup1=(nbrs[i],nbrs[j])
                        tup2=(tup1,[v])
                        output.append(tup2)
        for i in range(len(nbrs)):
                tup1=(v,nbrs[i])
                tup2=(tup1,[-1])
                output.append(tup2)
        return output



def func2(entry):
        output=[]
        pair=entry[0]
        nbrs=entry[1]
        v1=pair[0]
        v2=pair[1]
        if -1 in nbrs:
                for i in range(len(nbrs)):
                        if (nbrs[i]!=-1 and nbrs[i]<v1 and nbrs[i]<v2):
                                output.append((nbrs[i],v1,v2))
        return output

output=file.flatMap(func1).reduceByKey(lambda p,q:p+q).flatMap(func2)
output.saveAsTextFile(sys.argv[2])