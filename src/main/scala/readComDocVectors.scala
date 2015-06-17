import org.apache.hadoop.io.Text
import tl.lin.data.array.IntArrayWritable
import org.apache.spark.storage.StorageLevel
// reading the compressed vectors
val inputFile = "docvectors/segment*/*"
var data = sc.sequenceFile(inputFile, classOf[Text], classOf[IntArrayWritable]).map{case (x, y) => (x.toString, y)}
// persist in memory
data.persist(StorageLevel.MEMORY_ONLY)
// printing only the keys 
//data.map(t=>t._1).saveAsTextFile("temp-out")

