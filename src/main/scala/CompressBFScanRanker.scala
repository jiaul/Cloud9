import org.clueweb.data.PForDocVector
import tl.lin.data.array.IntArrayWritable
// assume that RDD <data> has the compressed docvectors
var docScore = data.map(t=> {
	val DOC = new PForDocVector()
	PForDocVector.fromIntArrayWritable(t._2, DOC)
	var array = DOC.getTermIds()
	var tf = new Array[Int](100)
	/** for(i <- 0 to 3) {
	 tf(i) = 0
	} */
	tf(0) = 0
	tf(1) = 0
	tf(2) = 0
	val adl = 750.0
	var doclen = array.length
	for(i <- 1 to (doclen - 1)) {
		if(array(i) == termidBCVar.value(0)) { 
		  tf(0) = tf(0) + 1
		}
		else if(array(i) == termidBCVar.value(1)) { 
		  tf(1) = tf(1) + 1
		}
		else if(array(i) == termidBCVar.value(2)) { 
		  tf(2) = tf(2) + 1
		}
		else {
		}
		   
		
	/** for(j <- 0 to 2) {
		if(array(i) == termidBCVar.value(j)) { 
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
