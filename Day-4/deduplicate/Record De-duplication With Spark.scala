// Databricks notebook source
// MAGIC %md # Address Resolution
// MAGIC Also known as entity resolution, entity disambiquation, record de-duplication.

// COMMAND ----------

// MAGIC %md ### 1. Problem Statement
// MAGIC Given a collection of records (addresses in our case), find records that represent the same entity.  
// MAGIC This is a difficult problem because the same entity can have different lexical (textual) representation, therefore direct string matching will fail to identify duplicates.

// COMMAND ----------

// DBTITLE 1,Example - Duplicate Address
case class company(no:Integer,company_name:String, address_line1:String, address_line2:String, post_town:String, postcode:String,description:String,company_status:String,company_category:String )

val address = Seq(
  company(1,"1 MOBILE LIMITED","30 CITY ROAD","","London","EC1Y 2AB","master record","",""),
  company(2,"1 MOBILE Ltd.","30 CITY RD","","","EC1Y 2AB","transaction record","Liquidation",""),
  company(3,"1 MOBILE","","CITY ROAD","London","","transaction record","","Private Limitted Company"))

val addressDF = sc.parallelize(address).toDF()

display(addressDF)

// COMMAND ----------

// MAGIC %md ## 2. Solution
// MAGIC In this solution we implement the naive approach.  
// MAGIC Technically important in this solution is **graph approach(transform relational data to graph)** to limit search space.  
// MAGIC The use of **motifs to find patterns** in the graph.  
// MAGIC **TF-IDF and similarity computation** 
// MAGIC The use of **Broadcast Variables**  
// MAGIC and other Interesting **SparkSQL** capabilities.
// MAGIC 
// MAGIC 
// MAGIC <img src ="https://s3.amazonaws.com/dataset201606/public/royal_mail/solution_pipeline.PNG"> </img>

// COMMAND ----------

// MAGIC %md ### The Graph  
// MAGIC Transforming relational data into a graph. In simple terms, a property graph represents data in vertices and edges.  
// MAGIC Here we re-organise our relational data into a graph so that entities(addresses) share nodes they have in common.
// MAGIC 
// MAGIC <img src= "https://s3.amazonaws.com/dataset201606/public/royal_mail/Copy+of+rLinkage+Graph+Model+(2).png"> </img>

// COMMAND ----------

// MAGIC %md ### 2.1 Load Data (Master File)
// MAGIC In this example I load a company address file from [Amazon S3](https://s3.amazonaws.com/dataset201606/public/royal_mail/companies.csv)  
// MAGIC This file is a random collection of company addresses from UK CompanyHouse file.

// COMMAND ----------

val postalAddressFile = sqlContext.read.option("header","true").csv("pathTo/companies.csv").toDF("id","company_name","address_line1","address_line2","post_town","county","postcode")

display(postalAddressFile.limit(5))

// COMMAND ----------

// DBTITLE 1,2.1 Load Data (Transaction File)
//creating a dummy duplicates for illustration.
case class company(id:String, company_name:String, address_line1:String, address_line2:String, post_town:String, county:String, postcode:String)

val transactionFile = sc.parallelize(Seq(
  company("1001","1 MOBILE LIMITED","30 CITY RD.","","","",""),
  company("1002","1 MOBILE Ltd.","30 CITY RD","","","","EC1Y 2AB"),
  company("1003","1 MOBILE","","CITY ROAD","London","","")) ).toDF("id","company_name","address_line1","address_line2","post_town","county","postcode")

display(transactionFile)

// COMMAND ----------

val masterTbl = postalAddressFile.selectExpr("id","company_name","concat(address_line1, ' ',address_line2,' ',post_town,' ', county,' ',postcode) as address","post_town","county","postcode")
val transactTbl = transactionFile.selectExpr("id","company_name","concat(address_line1, ' ',address_line2,' ',post_town,' ', county,' ',postcode) as address","post_town","county","postcode")

//in production, partition by post_town/post code ... keep related data in same partition and minimise shuffles later.

// COMMAND ----------

// DBTITLE 1,2.2 Create Graph
// Tranform relational data into graph
// UDF (user defined functions used to prepare text for graph creation)
val clean =  (x: String) => {
   val s = x.replace(" ","").trim().toLowerCase()
  s
 }

val hashid = (y: String) => {
  val h = y.replace(" ","").trim().toLowerCase().hashCode()
  val i = (h & 0x7FFFFFFF) % 2000000
  i
}
sqlContext.udf.register("udfClean", clean )
sqlContext.udf.register("udfHashIndex", hashid )

// COMMAND ----------

//Graphframe from master table....can be implemented better this is a direct mapping to graph. Every column becomes a property.
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.graphframes._

val entityV = masterTbl.selectExpr("id as id", "lower(company_name) as name", "'master' as label", "address as detail" )
val nameV = masterTbl.selectExpr("udfClean(company_name) as id", "lower(company_name) as name", "'name' as label", "'null' as detail").distinct()
val addressV = masterTbl.selectExpr("udfHashIndex(address) as id", "lower(address) as name", "'address' as label", "'null' as detail").distinct()
val cityV = masterTbl.where("post_town != 'null'").selectExpr("udfClean(post_town) as id", "lower(post_town) as name", "'city' as label", "'null' as detail").distinct()
val countyV = masterTbl.where("county != 'null'").selectExpr("udfClean(county) as id", "lower(county) as name", "'county' as label", "'null' as detail").distinct()
val postcodeV = masterTbl.where("postcode != 'null'").selectExpr("udfClean(postcode) as ID", "lower(postcode) as name", "'postcode' as label", "'null' as detail").distinct()

// creating vertices
val v1 = entityV.union(nameV).union(addressV).union(cityV).union(countyV).union(postcodeV).toDF()


val hasName = masterTbl.selectExpr("id as src","udfClean(company_name) as dst", "'hasName' as eProperty")
val hasAddress = masterTbl.selectExpr("id as src","udfHashIndex(address) as dst", "'hasAddress' as eProperty")
val hasCity = masterTbl.where("post_town != 'null'").selectExpr("id as src","udfClean(post_town) as dst", "'hasCity' as eProperty")
val hasCounty = masterTbl.where("county != 'null'").selectExpr("id as src","udfClean(county) as dst", "'hasCounty' as eProperty")
val hasPostcode = masterTbl.where("postcode != 'null'").selectExpr("ID as src","udfClean(postcode) as dst", "'hasPostCode' as eProperty")

// creating edges
val e1 = hasName.union(hasAddress).union(hasCounty).union(hasCity).union(hasPostcode).toDF()


display(v1.limit(5))

val g1 = GraphFrame(v1, e1)

// COMMAND ----------

//Graphframe from the Transactional Table. 

val tentityV = transactTbl.selectExpr("id as id", "lower(company_name) as name", "'transact' as label", "address as detail" )
val tnameV = transactTbl.selectExpr("udfClean(company_name) as id", "lower(company_name) as name", "'name' as label", "'null' as detail").distinct()
val taddressV = transactTbl.selectExpr("udfHashIndex(address) as id", "lower(address) as name", "'address' as label", "'null' as detail").distinct()
val tcityV = transactTbl.where("post_town not like ''").selectExpr("udfClean(post_town) as id", "lower(post_town) as name", "'city' as label", "'null' as detail").distinct()
val tcountyV = transactTbl.where("county not like ''").selectExpr("udfClean(county) as id", "lower(county) as name", "'county' as label", "'null' as detail").distinct()
val tpostcodeV = transactTbl.where("postcode not like ''").selectExpr("udfClean(postcode) as ID", "lower(postcode) as name", "'postcode' as label", "'null' as detail").distinct()

val v2 = tentityV.union(tnameV).union(taddressV).union(tcityV).union(tcountyV).union(tpostcodeV).toDF()


val thasName = transactTbl.selectExpr("id as src","udfClean(company_name) as dst", "'hasName' as eProperty")
val thasAddress = transactTbl.selectExpr("id as src","udfHashIndex(address) as dst", "'hasAddress' as eProperty")
val thasCity = transactTbl.where("post_town not like ''")selectExpr("id as src","udfClean(post_town) as dst", "'hasCity' as eProperty")
val thasCounty = transactTbl.where("county not like ''").selectExpr("id as src","udfClean(county) as dst", "'hasCounty' as eProperty")
val thasPostcode = transactTbl.where("postcode not like ''").selectExpr("ID as src","udfClean(postcode) as dst", "'hasPostCode' as eProperty")


val e2 = thasName.union(thasAddress).union(thasCounty).union(thasCity).union(thasPostcode).toDF()


display(e2)

val g2 = GraphFrame(v2, e2)

// COMMAND ----------

val gf = GraphFrame(g1.vertices.union(g2.vertices), g1.edges.union(g2.edges))

gf.vertices.cache()
gf.edges.cache()

println("V: " + gf.vertices.count)
println("E: " + gf.edges.count)

// COMMAND ----------

// MAGIC %md ### 2.3 Find Potential Duplicates
// MAGIC Using **GraphFrame Motifs** we breakup the search space and identify **potential duplicates**.

// COMMAND ----------

val motifs = gf.find("(a)-[ae1]->(p1); (b)-[be1]->(p1)") // find entities that point to a common property(p1).

// COMMAND ----------

val pLinked = motifs.where("a.label = 'master' and b.label = 'transact' and  ae1.eProperty=be1.eProperty")
.selectExpr("ae1", "be1", "a.id as aId", "b.id as bId","concat(a.name,' ', a.detail) as aDetail", "concat(b.name,' ',b.detail) as bDetail")
.distinct()

pLinked.cache()

display(pLinked.limit(5))

// COMMAND ----------

// MAGIC %md ### 2.4 TF-IDF Computation
// MAGIC TF-IDF computed on potential duplicates.   
// MAGIC Limitting the search space before doing this improves our chances.

// COMMAND ----------

val tfidfData = pLinked.select("aId","aDetail").union(pLinked.select("bId","bDetail")).distinct()

display(tfidfData.limit(10))

// COMMAND ----------

import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer, RegexTokenizer}
import org.apache.spark.ml.{Pipeline, PipelineModel}
val tokenizer = new RegexTokenizer()
  .setPattern("[\\W_]+") // regex to match word and numbers.
  .setInputCol("aDetail")
  .setOutputCol("words")

//intialise TF
val hashingTF = new HashingTF()
  .setInputCol("words")
  .setOutputCol("rawFeatures")
  .setNumFeatures(50000)

val idf = new IDF()
  .setInputCol("rawFeatures")
  .setOutputCol("features")

val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, idf))
val model = pipeline.fit(tfidfData)

// COMMAND ----------

import org.apache.spark.ml.linalg.SparseVector
val tfidfDataV = model.transform(tfidfData)

val addressFeatureVectors = tfidfDataV.selectExpr("aId as id","features")

//Broadcast variable to lookup vector for comparism.
val bAddressFeatureVectors = sc.broadcast(addressFeatureVectors.rdd.map{case Row(x:String, y:SparseVector) => (x,y) }.collectAsMap())

// COMMAND ----------

case class compare(aid:String, bid:String, adetail:String, bdetail:String, afeatures:SparseVector, bfeatures:SparseVector)

val compareDF = pLinked.select("aId","bId","aDetail","bDetail").rdd
   .map{case Row(x:String,y:String,u:String,v:String) => compare(x,y,u,v,bAddressFeatureVectors.value(x),bAddressFeatureVectors.value(y))}.toDF()

//display(compareDF.limit(5))

// COMMAND ----------

// MAGIC %md ### 2.5 Compute Similarity
// MAGIC Using cosine similarity of the TF-IDF vectors.

// COMMAND ----------

import org.apache.spark.ml.linalg.SparseVector

def dotProduct(v: Map[Int,Double], u: Map[Int,Double]): Double = {
    (for( (a,b) <- v if u.keySet.contains(a)) yield v(a)*u(a)) sum
}

def magnitude(v: Map[Int,Double]): Double = {
   math.sqrt(dotProduct(v, v))
}

def cosim(v: Map[Int,Double], u: Map[Int,Double]): Double = {
  dotProduct(v,u)/magnitude(v)/magnitude(u)
}
// UDF function
val cosineSimilarity =  (x: SparseVector, y:SparseVector) => {
   
 val v1 = (x.indices zip x.values).toMap
 val v2 = (y.indices zip y.values).toMap
  
  cosim(v1,v2)
 }
sqlContext.udf.register("cSim",cosineSimilarity)

// COMMAND ----------

val pLinkedSim = compareDF.selectExpr("aid","bid","adetail","bdetail","cSim(afeatures,bfeatures) as similarity" ).sort($"similarity".desc )
display(pLinkedSim.limit(5))

// COMMAND ----------

val pLinkedSim = compareDF.selectExpr("aid","bid","adetail","bdetail","cSim(afeatures,bfeatures) as similarity","afeatures","bfeatures" ).sort($"similarity".desc )
display(pLinkedSim.limit(5))

// COMMAND ----------

// MAGIC %md ### 2.6 Solution - Recommend Best Match
// MAGIC As expected the results show all the badly captured addresses are best related to "master address id = 1"

// COMMAND ----------

val ldata = pLinkedSim.selectExpr("aid","bid","row_number() over (partition by bid order by Similarity desc) as rank", "adetail","bdetail","similarity")
.where("rank =1").sort($"similarity".desc)
display(ldata.select("aid","bid","adetail","bdetail","similarity","rank"))

// COMMAND ----------

display(transactTbl)

// COMMAND ----------

display(masterTbl.where("id = '1'"))
