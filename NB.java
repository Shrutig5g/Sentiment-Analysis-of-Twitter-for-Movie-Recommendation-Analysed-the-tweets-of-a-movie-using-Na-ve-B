import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//Driver Code that runs on client to configure and submit the job
public class NB extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {

    if (args.length != 2) {
      System.out.printf(
          "Usage: %s [generic options] <input dir> <output dir>\n", getClass()
              .getSimpleName());
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    //creation of job
    JobConf conf = new JobConf(getConf(), NB.class);
    conf.setJobName(this.getClass().getName());

    //specifying no of input and output paths
    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    //specifying class name of Mapper and Reducer
    conf.setMapperClass(NBMapper.class);
    conf.setReducerClass(NBReducer.class);

    //output data types for Mapper
    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(DoubleWritable.class);
	
    //output data types of Reducer
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(DoubleWritable.class);

    
    JobClient.runJob(conf);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new NB(),args);
    System.exit(exitCode);
  }
}
