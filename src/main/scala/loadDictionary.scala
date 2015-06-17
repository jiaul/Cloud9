import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.clueweb.clueweb12.app.BuildDictionary;
import org.clueweb.data.TermStatistics;
import org.clueweb.dictionary._;
// dictionary path
val path = "dictionary"
val conf = new Configuration()
val fs = FileSystem.get(conf)
val dictionary = new DefaultFrequencySortedDictionary(path, fs)
val stats = new TermStatistics(new Path(path), fs)
val query = new Array[String](100)
// example query
query(0)="barack";query(1)="obama";query(2)="famili"
var termid = new Array[Int](100)
var idf = new Array[Double](100)
// collect term id and df
for(i <- 0 to 2) {
	termid(i) = dictionary.getId(query(i))
	idf(i) = scala.math.log((1.0*52343021)/stats.getDf(termid(i)))
}
// broadcast the termid array and idf array to every worker (NOTE: the data to be broadcasted must be declared <var> ??)
val termidBCVar = sc.broadcast(termid)
val idfBCVar = sc.broadcast(idf)

 


