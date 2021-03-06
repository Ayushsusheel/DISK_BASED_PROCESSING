
/*WELCOME USERS!!!!!!!!!!!!!

After setting up the cloudera on our virtual machine,
follow the procedure ,

 *  firstly open up the eclipse in our cloudera------------------>

 * then,create a new java project say project name is WordCount3
 
 *now click next not finish then select the option libraries go to file system inside that select usr inside usr select lib inside lib select hadoop select all the jar files present 
 then click OK again click on add external jar's  within the same folder now go to client folder which is present in hadoop folder itself!!copy all jar files then simply click OK!! then  [  FINISH  ]
  
 * now select the Java project WordCount3 inside src create a class name same as the project name i.e WordCount3
 
 *if package is showing simply click on browse select the default one....then FINISHH.....*/
 

   
   
 //=============================================================================C O D E ===============================================================================================//  
   
  // *now the SOURCE CODE is----->
 
 
 import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount3 {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount3.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}



//========================================================================================================================================================================================================//
/*
* NOW THE MAIN PROCESSSSSSSSSSSSSSSSS

EXPORT------>

click on WordCount3 project right click select EXPORT (we have to export this to cloudera folder) now select the JAVA inside this select JAR file select next 

*   NOW in the export destination browse the location click browse...

*  now, click file system inside this select home then cloudera then click in it then change the jar file name as WordCount3.jar then click ok

does it appear like this--->/home/cloudera/WordCount3.jar

great going now,,

now click finish.

* now for verifying open computer folder in cloudera ,inside that click file system then home,cloudera ull see a jar file named as WordCount3......

* close this terminal 

* now open up a new terminal now we need to create a text file which will store our data for WORD COUNT.....


*  cat > /home/cloudera/file.txt---------------------------------------------------------------------------------------------------->command for creating a txt file....
aaaaaaaaaaaa
AAAAAAAAAA
A A A A A
HELLLO
HE LL O O 0 
12 3 3 3 455 6 7 
^Z------------------------------------------------------------------------------------>press ctrl+Z if u think words are enough...
[1]+  Stopped                 cat > /home/cloudera/file.txt

now,

we need to check whether our work is saved or not


[cloudera@quickstart ~]$ cat  /home/cloudera/file.txt------------------------------VIEWING THE DATA
aaaaaaaaaaaa
AAAAAAAAAA
A A A A A
HELLLO
HE LL O O 0 
12 3 3 3 455 6 7 

now,

we have to create a directory inside which we will store our text file by using the command

[cloudera@quickstart ~]$ hdfs dfs -mkdir /folder3--------------------------------------------------------------->CREATING DIRECTORY ( present in our local file system)


now we need to put our text file in the directory
[cloudera@quickstart ~]$ hdfs dfs -put /home/cloudera/file.txt /folder3 -------------------------PUT COMMAND


check whether the file is shifted to our folder or not,


[cloudera@quickstart ~]$ hdfs dfs -cat /folder3/*---------------------------------------------VIEW DATA PRESENT INSIDE OUR DIRECTORY
aaaaaaaaaaaa
AAAAAAAAAA
A A A A A
HELLLO
HE LL O O 0 
12 3 3 3 455 6 7 





// [cloudera@quickstart ~]$ hadoop jar /home/cloudera/WordCount3.jar WordCount3 /folder3/file.txt /OUTPUT_FOLDER3-------------------------------->FINAL command this will use our jar file

// which we have created using input as file.txt file and it will create the output into new folder named as OUTPUT_FOLDER3 (we can give any offcourse!!)

NOW PRESS ENTER----------------------------------->

21/08/19 10:13:53 INFO client.RMProxy: Connecting to ResourceManager at /0.0.0.0:8032
21/08/19 10:13:54 WARN mapreduce.JobResourceUploader: Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
21/08/19 10:13:55 INFO input.FileInputFormat: Total input paths to process : 1
21/08/19 10:13:55 INFO mapreduce.JobSubmitter: number of splits:1
21/08/19 10:13:56 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1616685848912_0082
21/08/19 10:13:56 INFO impl.YarnClientImpl: Submitted application application_1616685848912_0082
21/08/19 10:13:56 INFO mapreduce.Job: The url to track the job: http://quickstart.cloudera:8088/proxy/application_1616685848912_0082/
21/08/19 10:13:56 INFO mapreduce.Job: Running job: job_1616685848912_0082
21/08/19 10:14:08 INFO mapreduce.Job: Job job_1616685848912_0082 running in uber mode : false
21/08/19 10:14:08 INFO mapreduce.Job:  map 0% reduce 0%
21/08/19 10:14:17 INFO mapreduce.Job:  map 100% reduce 0%
21/08/19 10:14:27 INFO mapreduce.Job:  map 100% reduce 100%
21/08/19 10:14:27 INFO mapreduce.Job: Job job_1616685848912_0082 completed successfully    <-----------------------------------[ SUCCESS ]
21/08/19 10:14:27 INFO mapreduce.Job: Counters: 49
	File System Counters
		FILE: Number of bytes read=140
		FILE: Number of bytes written=286983
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
		HDFS: Number of bytes read=185
		HDFS: Number of bytes written=82
		HDFS: Number of read operations=6
		HDFS: Number of large read operations=0
		HDFS: Number of write operations=2
	Job Counters 
		Launched map tasks=1
		Launched reduce tasks=1
		Data-local map tasks=1
		Total time spent by all maps in occupied slots (ms)=6007
		Total time spent by all reduces in occupied slots (ms)=6245
		Total time spent by all map tasks (ms)=6007
		Total time spent by all reduce tasks (ms)=6245
		Total vcore-milliseconds taken by all map tasks=6007
		Total vcore-milliseconds taken by all reduce tasks=6245
		Total megabyte-milliseconds taken by all map tasks=6151168
		Total megabyte-milliseconds taken by all reduce tasks=6394880
	Map-Reduce Framework
		Map input records=6
		Map output records=20
		Map output bytes=150
		Map output materialized bytes=140
		Input split bytes=113
		Combine input records=20
		Combine output records=13
		Reduce input groups=13
		Reduce shuffle bytes=140
		Reduce input records=13
		Reduce output records=13
		Spilled Records=26
		Shuffled Maps =1
		Failed Shuffles=0
		Merged Map outputs=1
		GC time elapsed (ms)=96
		CPU time spent (ms)=3650
		Physical memory (bytes) snapshot=514596864
		Virtual memory (bytes) snapshot=3147784192
		Total committed heap usage (bytes)=389545984
	Shuffle Errors
		BAD_ID=0
		CONNECTION=0
		IO_ERROR=0
		WRONG_LENGTH=0
		WRONG_MAP=0
		WRONG_REDUCE=0
	File Input Format Counters 
		Bytes Read=72
	File Output Format Counters 
		Bytes Written=82

NOW,

[cloudera@quickstart ~]$ hdfs dfs -ls /OUTPUT_FOLDER3Found 2 items---------------------------------------------------->view what is present in the output folder 
-rw-r--r--   1 cloudera supergroup          0 2021-08-19 10:14 /OUTPUT_FOLDER3/_SUCCESS
-rw-r--r--   1 cloudera supergroup         82 2021-08-19 10:14 /OUTPUT_FOLDER3/part-r-00000


now simply just write,

[cloudera@quickstart ~]$ hdfs dfs -cat /OUTPUT_FOLDER3/part-r-00000-----------------------------------> [   MAP REDUCE WORD COUNT OUTPUT  ]
0	1
12	1
3	3
455	1
6	1
7	1
A	5
AAAAAAAAAA	1
HE	1
HELLLO	1
LL	1
O	2
aaaaaaaaaaaa	1
[cloudera@quickstart ~]$ */














































