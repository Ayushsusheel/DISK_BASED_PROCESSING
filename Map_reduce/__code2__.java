/*WELCOME USERS!!!!!!!!!!!!!

After setting up the cloudera on our virtual machine,
follow the procedure ,

 *  firstly open up the eclipse in our cloudera------------------>

 * then,create a new java project say project name is mapreduce
 
 *now click next not finish then select the option libraries go to file system inside that select usr inside usr select lib inside lib select hadoop select all the jar files present 
 then click OK again click on add external jar's  within the same folder now go to client folder which is present in hadoop folder itself!!copy all jar files then simply click OK!! then  [  FINISH  ]
  
 * now select the Java project WordCount3 inside src create a class name same as the project name i.e WordCount3
 
 *if package is showing simply click on browse select the default one....then FINISHH.....
 
 *now the source code is----->*/
 
 //==============================================================================C  O  D  E ====================================================================================================
 



import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class mapreduce {
    public static class IntPair
	implements WritableComparable<IntPair> {
	private int first = 0;
	private int second = 0;
    
	/**
	 * Set the left and right values.
	 */
	public void set(int left, int right) {
	    first = left;
	    second = right;
	}
	public int getFirst() {
	    return first;
	}
	public int getSecond() {
	    return second;
	}
	/**
	 * Read the two integers. 
	 * Encoded as: MIN_VALUE -> 0, 0 -> -MIN_VALUE, MAX_VALUE-> -1
	 */
	@Override
	    public void readFields(DataInput in) throws IOException {
	    first = in.readInt() + Integer.MIN_VALUE;
	    second = in.readInt() + Integer.MIN_VALUE;
	}
	@Override
	    public void write(DataOutput out) throws IOException {
	    out.writeInt(first - Integer.MIN_VALUE);
	    out.writeInt(second - Integer.MIN_VALUE);
	}
	@Override
	    public int hashCode() {
	    return first * 157 + second;
	}
	@Override
	    public boolean equals(Object right) {
	    if (right instanceof IntPair) {
		IntPair r = (IntPair) right;
		return r.first == first && r.second == second;
	    } else {
		return false;
	    }
	}
	/** A Comparator that compares serialized IntPair. */ 
	public static class Comparator extends WritableComparator {
	    public Comparator() {
		super(IntPair.class);
	    }

	    public int compare(byte[] b1, int s1, int l1,
			       byte[] b2, int s2, int l2) {
		return compareBytes(b1, s1, l1, b2, s2, l2);
	    }
	}

	static {                                        // register this comparator
	    WritableComparator.define(IntPair.class, new Comparator());
	}

	@Override
	    public int compareTo(IntPair o) {
	    if (first != o.first) {
		return first < o.first ? -1 : 1;
	    } else if (second != o.second) {
		return second < o.second ? -1 : 1;
	    } else {
		return 0;
	    }
	}
    }
  
    // maps word to (firstAlphabet -> (lengthOfWordsSoFar, numberOfWords))
    public static class TokenizerMapper
	extends Mapper<Object, Text, Text, IntPair>{

	private Text firstCharacter = new Text();
	private final static IntPair sumAndCount = new IntPair();

	public void map(Object key, Text value, Context context)
	    throws IOException, InterruptedException {
	    StringTokenizer itr = new StringTokenizer(value.toString());
	    while (itr.hasMoreTokens()) {
		String token = itr.nextToken();
		firstCharacter.set(token.substring(0,1));
		// partial: <sum, numElements>
		sumAndCount.set(token.length(), 1);
		context.write(firstCharacter, sumAndCount);
	    }
	}
    }

    // combiner class
    public static class IntPairPartialSumCombiner
	extends Reducer<Text,IntPair,Text,IntPair> {
	private IntPair result = new IntPair();

	public void reduce(Text key, Iterable<IntPair> values,
			   Context context
			   ) throws IOException, InterruptedException {
	    int sum = 0;
	    int total = 0;
	    for (IntPair val : values) {
		sum += val.getFirst();
		total += val.getSecond();
	    }
	    result.set(sum, total);
	    context.write(key, result);
	}
    }

    // reducer class
    public static class IntPairAverageReducer
	extends Reducer<Text, IntPair, Text, DoubleWritable> {
	private DoubleWritable result = new DoubleWritable();

	public void reduce(Text key, Iterable<IntPair> values,
			   Context context
			   ) throws IOException, InterruptedException {
	    double sum = 0;
	    int total = 0;
	    for (IntPair val : values) {
		sum += val.getFirst();
		total += val.getSecond();
	    }
	    double average = sum / total;
	    result.set(average);
	    context.write(key, result);
	}
    }

    public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	if (otherArgs.length != 2) {
	    System.err.println("Usage: wordcount <in> <out>");
	    System.exit(2);
	}
	Job job = new Job(conf, "word count");
	job.setJarByClass(mapreduce.class);
	job.setMapperClass(TokenizerMapper.class);
	job.setCombinerClass(IntPairPartialSumCombiner.class);
	job.setReducerClass(IntPairAverageReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntPair.class);
	FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


 //==================================================================================================================================================================================
 
/*
* NOW THE MAIN PROCESSSSSSSSSSSSSSSSS

EXPORT------>

click on mapreduce project right click select EXPORT (we have to export this to cloudera folder) now select the JAVA inside this select JAR file select next 

*   NOW in the export destination browse the location click browse...

*  now, click file system inside this select home then cloudera then click in it then change the jar file name as WordCount3.jar then click ok

does it appear like this--->/home/cloudera/mapreduce.jar

great going now,,

now click finish.

* now for verifying open computer folder in cloudera ,inside that click file system then home,cloudera ull see a jar file named as mapreduce......

* close this terminal 

* now open up a new terminal now we need to create a text file which will store our data for MAP + REDUCE.....


                                                                  
* [cloudera@quickstart ~]$ cat > /home/cloudera/mapfile.txt ---------------------------------------------------------------------------->command for creating a txt file....
no
not
now
note
nothing
nobody
never    
^Z
[1]+  Stopped                 cat > /home/cloudera/mapfile.txt------------------------------------------------------------->press ctrl+Z if u think words are enough...

 
 
 
now,

we need to check whether our work is saved or not


[cloudera@quickstart ~]$ cat  /home/cloudera/mapfile.txt------------------------------VIEWING THE DATA
no
not
now
note
nothing
nobody
never


now,

we have to create a directory inside which we will store our text file by using the command

[cloudera@quickstart ~]$ hdfs dfs -mkdir /mapfolder--------------------------------------------------------------->CREATING DIRECTORY ( present in our local file system)


now we need to put our text file in the directory
[cloudera@quickstart ~]$ hdfs dfs -put /home/cloudera/mapfile.txt /mapfolder -------------------------PUT COMMAND


check whether the file is shifted to our folder or not,


[cloudera@quickstart ~]$ hdfs dfs -cat /mapfolder/*---------------------------------------------VIEW DATA PRESENT INSIDE OUR DIRECTORY
no
not
now
note
nothing
nobody
never


[cloudera@quickstart ~]$ hadoop jar /home/cloudera/mapreduce.jar mapreduce /mapfolder/mapfile.txt /mapoutput-------------------------------->FINAL command this will use our jar file___

                                                                                                                                             ___which we have created using input as mapfile.txt file and it will create the output into new folder named as mapoutput (we can give any name ofcourse!!)

NOW PRESS ENTER----------------------------------->


21/08/24 00:06:41 INFO client.RMProxy: Connecting to ResourceManager at /0.0.0.0:8032
21/08/24 00:06:42 INFO input.FileInputFormat: Total input paths to process : 1
21/08/24 00:06:42 INFO mapreduce.JobSubmitter: number of splits:1
21/08/24 00:06:42 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1616685848912_0083
21/08/24 00:06:42 INFO impl.YarnClientImpl: Submitted application application_1616685848912_0083
21/08/24 00:06:42 INFO mapreduce.Job: The url to track the job: http://quickstart.cloudera:8088/proxy/application_1616685848912_0083/
21/08/24 00:06:42 INFO mapreduce.Job: Running job: job_1616685848912_0083
21/08/24 00:06:54 INFO mapreduce.Job: Job job_1616685848912_0083 running in uber mode : false
21/08/24 00:06:54 INFO mapreduce.Job:  map 0% reduce 0%
21/08/24 00:07:05 INFO mapreduce.Job:  map 100% reduce 0%
21/08/24 00:07:15 INFO mapreduce.Job:  map 100% reduce 100%
21/08/24 00:07:15 INFO mapreduce.Job: Job job_1616685848912_0083 completed successfully---------------------------------------[S U C C E S S ]
21/08/24 00:07:15 INFO mapreduce.Job: Counters: 49
	File System Counters
		FILE: Number of bytes read=18
		FILE: Number of bytes written=287095
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
		HDFS: Number of bytes read=155
		HDFS: Number of bytes written=20
		HDFS: Number of read operations=6
		HDFS: Number of large read operations=0
		HDFS: Number of write operations=2
	Job Counters 
		Launched map tasks=1
		Launched reduce tasks=1
		Data-local map tasks=1
		Total time spent by all maps in occupied slots (ms)=9342
		Total time spent by all reduces in occupied slots (ms)=6229
		Total time spent by all map tasks (ms)=9342
		Total time spent by all reduce tasks (ms)=6229
		Total vcore-milliseconds taken by all map tasks=9342
		Total vcore-milliseconds taken by all reduce tasks=6229
		Total megabyte-milliseconds taken by all map tasks=9566208
		Total megabyte-milliseconds taken by all reduce tasks=6378496
	Map-Reduce Framework
		Map input records=7
		Map output records=7
		Map output bytes=70
		Map output materialized bytes=18
		Input split bytes=118
		Combine input records=7
		Combine output records=1
		Reduce input groups=1
		Reduce shuffle bytes=18
		Reduce input records=1
		Reduce output records=1
		Spilled Records=2
		Shuffled Maps =1
		Failed Shuffles=0
		Merged Map outputs=1
		GC time elapsed (ms)=80
		CPU time spent (ms)=2300
		Physical memory (bytes) snapshot=489250816
		Virtual memory (bytes) snapshot=3150282752
		Total committed heap usage (bytes)=388497408
	Shuffle Errors
		BAD_ID=0
		CONNECTION=0
		IO_ERROR=0
		WRONG_LENGTH=0
		WRONG_MAP=0
		WRONG_REDUCE=0
	File Input Format Counters 
		Bytes Read=37
	File Output Format Counters 
		Bytes Written=20



NOW,
[cloudera@quickstart ~]$ hdfs dfs -ls /mapoutput
Found 2 items------------------------------------------------------------------------------------------>we are viewing what is present in the output folder 
-rw-r--r--   1 cloudera supergroup          0 2021-08-24 00:07 /mapoutput/_SUCCESS
-rw-r--r--   1 cloudera supergroup         20 2021-08-24 00:07 /mapoutput/part-r-00000



now simply just write,

[cloudera@quickstart ~]$ hdfs dfs -cat /mapoutput/part-r-00000
n	4.285714285714286-----------------------------------> [   MAP REDUCE  OUTPUT  ]




As,our input is this--->
no
not
now
note
nothing
nobody
never
















