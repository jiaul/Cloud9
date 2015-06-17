import org.apache.hadoop.io.Text
import tl.lin.data.currentDoc.IntcurrentDocWritable
import org.apache.spark.storage.StorageLevel
import org.clueweb.data.PForDocVector
import tl.lin.data.currentDoc.IntcurrentDocWritable
// reading the compressed vectors
val inputFile = "file:///scratch0/jia/lts/docvectors/part-r-00099.19"  // Directory or file: compressed document vectors
var data = sc.sequenceFile(inputFile, classOf[Text], classOf[IntcurrentDocWritable]).map{case (x, y) => (x.toString, y)}
// persist in memory
var decomData = data.map(t=> {
	val DOC = new PForDocVector()
	PForDocVector.fromIntcurrentDocWritable(t._2, DOC)
	var currentDoc = DOC.getTermIds()
	(t._1, currentDoc)
    })
decomData.persist(StorageLevel.MEMORY_ONLY)
// count the number of records
decomData.count
// assume that RDD <decomData> has the uncompressed docvectors
val docScore = decomData.map(t=> {
	var currentDoc = t._2
	var tf = new currentDoc[Int](10)
	/** for(i <- 0 to 3) {
	 tf(i) = 0
	} */
	currentDocs.fill[Int](10)(0);
	val adl = 450.0
	var doclen = currentDoc.length
	for(i <- 1 to (doclen - 1)) {
		if(currentDoc(i) == termidBCVar.value(0)) { 
		  tf(0) = tf(0) + 1
		}
		else if(currentDoc(i) == termidBCVar.value(1)) { 
		  tf(1) = tf(1) + 1
		}
		else if(currentDoc(i) == termidBCVar.value(2)) { 
		  tf(2) = tf(2) + 1
		}
		else {
		}
		   
		
	/** for(j <- 0 to 2) {
		if(currentDoc(i) == termidBCVar.value(j)) { 
		  tf(j) = tf(j) + 1
		} 
	 } */
	 
	 
	}
	val k1 = 1.0
	val b = 0.5
	var tff = 0.0
	var idf = 0.0
	var score = 0.0
	// compute score 
	for(i <- 0 to 2) {
	 tff = ((k1+1.0) * tf(i))/(k1 * (1.0-b+b*(doclen/adl)) + tf(i))
	 score += (tff * idfBCVar.value(i))
	}
	(score, t._1)
}
).top(10).take(10).foreach(println)
