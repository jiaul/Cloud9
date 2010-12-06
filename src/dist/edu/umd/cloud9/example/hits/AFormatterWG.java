/**
 * 
 */
package ivory.hits;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.PairOfStrings;
import edu.umd.cloud9.io.ArrayListWritable;
import edu.umd.cloud9.io.ArrayListOfIntsWritable;
import edu.umd.cloud9.util.HMapIV;
import edu.umd.cloud9.util.MapIV;
import ivory.hits.HITSNode;

/**
 * @author michaelmcgrath
 *
 */
public class AFormatterWG extends Configured implements Tool {
	
	private static final Logger sLogger = Logger.getLogger(AFormatterWG.class);
	
	/**
	 * @param args
	 */
	private static class AFormatMapper extends MapReduceBase implements
	Mapper<LongWritable, Text, IntWritable, HITSNode>
	{
		private HITSNode valOut = new HITSNode();
		private IntWritable keyOut = new IntWritable();
		HashSet<Integer> stopList = new HashSet<Integer>();
		
		public void configure (JobConf jc)
		{
			stopList = readStopList(jc);
		}
		
		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, HITSNode> output, Reporter reporter) throws IOException {
			
			ArrayListOfIntsWritable links = new ArrayListOfIntsWritable();
			String line = ((Text) value).toString();
			StringTokenizer itr = new StringTokenizer(line);
			if (itr.hasMoreTokens()) {
				int curr = Integer.parseInt(itr.nextToken());
				if ( stopList.contains(curr) )
				{
					return;
				}
				valOut.setAdjacencyList(links);
				valOut.setHARank((float) 1.0);
				valOut.setType(HITSNode.TYPE_AUTH_COMPLETE);
			}
			while (itr.hasMoreTokens()) {
				keyOut.set(Integer.parseInt(itr.nextToken()));
				valOut.setNodeId(keyOut.get());
				//System.out.println(keyOut.toString() + ", " + valOut.toString());
				if ( !(stopList.contains(keyOut.get())))
				{
					output.collect(keyOut, valOut);
				}
			}
			//emit mentioned mentioner -> mentioned (mentioners) in links
			//emit mentioner mentioned -> mentioner (mentions) outlinks
			//emit mentioned a
			//emit mentioner 1
		}
		
	}
	
	private static class AFormatMapperIMC extends MapReduceBase implements
	Mapper<LongWritable, Text, IntWritable, HITSNode>
	{
		private HITSNode valOut = new HITSNode();
		private IntWritable keyOut = new IntWritable();
		private static OutputCollector<IntWritable, HITSNode> mOutput;
		private static HMapIV<ArrayListOfIntsWritable> adjLists = new HMapIV<ArrayListOfIntsWritable>();
		Path [] cacheFiles;
		HashSet<Integer> stopList = new HashSet<Integer>();
		
		public void configure (JobConf jc)
		{
			stopList = readStopList(jc);
			adjLists.clear();
		}
		
		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, HITSNode> output, Reporter reporter) throws IOException {
			
			mOutput = output;
			
			ArrayListOfIntsWritable links = new ArrayListOfIntsWritable();
			String line = ((Text) value).toString();
			StringTokenizer itr = new StringTokenizer(line);
			if (itr.hasMoreTokens()) {
				int curr = Integer.parseInt(itr.nextToken());
				if ( !(stopList.contains(curr)) )
				{
					links.add(curr);
				}
				else
				{
					return;
				}
				//add to HMap here	
			}
			while (itr.hasMoreTokens()) {
				int curr = Integer.parseInt(itr.nextToken());
				if ( !(stopList.contains(curr)) ) {
					if (adjLists.containsKey(curr))
					{
						ArrayListOfIntsWritable list = adjLists.get(curr);
						list.trimToSize();
						links.trimToSize();
						list.addAll(links.getArray());
						adjLists.put(curr, list);
					}
					else
					{
						links.trimToSize();
						adjLists.put(curr, links);
					}
				}
			}
		}
		
		public void close() throws IOException
		{
			for (MapIV.Entry<ArrayListOfIntsWritable> e : adjLists.entrySet())
			{
				keyOut.set(e.getKey());
				valOut.setNodeId(e.getKey());
				valOut.setHARank((float) 0.0);
				valOut.setType(HITSNode.TYPE_AUTH_COMPLETE);
				valOut.setAdjacencyList(e.getValue());
				mOutput.collect(keyOut, valOut);
			}
		}
		
	}
	
	private static class AFormatReducer extends MapReduceBase implements
	Reducer<IntWritable, HITSNode, IntWritable, HITSNode>
	{
		private HITSNode valIn;
		private HITSNode valOut = new HITSNode();
		ArrayListOfIntsWritable adjList = new ArrayListOfIntsWritable();
		
		public void reduce(IntWritable key, Iterator<HITSNode> values, OutputCollector<IntWritable, HITSNode> output,
				Reporter reporter) throws IOException
		{
			//ArrayListOfIntsWritable adjList = new ArrayListOfIntsWritable();
			adjList.clear();
			
			//System.out.println(key.toString());
			//System.out.println(adjList.toString());
			while (values.hasNext())
			{
				valIn = values.next();
				ArrayListOfIntsWritable adjListIn = valIn.getAdjacencyList();
				adjListIn.trimToSize();
				adjList.addAll( adjListIn.getArray());
				//System.out.println(adjList.toString());
			}
			
			valOut.setType(HITSNode.TYPE_AUTH_COMPLETE);
			valOut.setHARank((float) 0.0);
			valOut.setAdjacencyList(adjList);
			valOut.setNodeId(key.get());
			
			output.collect(key, valOut);
			
		}
	}
	
	private static int printUsage() {
		System.out.println("usage: [input-path] [output-path] [num-mappers] [num-reducers]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}
	
	public int run(String[] args) throws Exception {
		
		if (args.length != 4) {
			printUsage();
			return -1;
		}

		String inputPath = args[0];
		String outputPath = args[1];

		int mapTasks = Integer.parseInt(args[2]);
		int reduceTasks = Integer.parseInt(args[3]);

		sLogger.info("Tool: AFormatter");
		sLogger.info(" - input path: " + inputPath);
		sLogger.info(" - output path: " + outputPath);
		sLogger.info(" - number of mappers: " + mapTasks);
		sLogger.info(" - number of reducers: " + reduceTasks);

		JobConf conf = new JobConf(AFormatterWG.class);
		conf.setJobName("Authority Formatter -- Web Graph");
		
		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		FileOutputFormat.setCompressOutput(conf, false);

		//conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(HITSNode.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		conf.setCompressMapOutput(true);
		conf.setSpeculativeExecution(false);
		//InputSampler.Sampler<IntWritable, Text> sampler = new InputSampler.RandomSampler<IntWritable, Text>(0.1, 10, 10);
		//InputSampler.writePartitionFile(conf, sampler);
		//conf.setPartitionerClass(TotalOrderPartitioner.class);
		conf.setMapperClass(AFormatMapperIMC.class);
		conf.setCombinerClass(AFormatReducer.class);
		conf.setReducerClass(AFormatReducer.class);

		// Delete the output directory if it exists already
		Path outputDir = new Path(outputPath);
		Path stopList = new Path("/tmp/mmcgrath/counts/toobig.txt");
		FileSystem.get(conf).delete(outputDir, true);
		
		long startTime = System.currentTimeMillis();
		sLogger.info("Starting job");
		DistributedCache.addCacheFile(stopList.toUri(), conf);
		JobClient.runJob(conf);
		sLogger.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");
		
		return 0;
	}
	
	private static HashSet<Integer> readStopList(JobConf jc) {
		HashSet<Integer> out = new HashSet<Integer> ();
		try {
			Path[] cacheFiles = DistributedCache.getLocalCacheFiles(jc);
			FileReader fr = new FileReader(cacheFiles[0].toString());
			BufferedReader stopReader = new BufferedReader(fr);
		    String line;
		    while ((line = stopReader.readLine()) != null) {
		        out.add(Integer.parseInt(line));
		    }
			stopReader.close();
			return out;
		} 
		catch (IOException ioe) {
	      System.err.println("IOException reading from distributed cache");
	      System.err.println(ioe.toString());
	      return out;
	    }
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new AFormatterWG(), args);
		System.exit(res);
	}

}