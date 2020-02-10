import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class NBReducer extends MapReduceBase implements
    Reducer<Text,DoubleWritable, Text, DoubleWritable> {

  @Override
  public void reduce(Text key, Iterator<DoubleWritable> values,
      OutputCollector<Text, DoubleWritable> output, Reporter reporter)
      throws IOException {

  Double wordCount =0.0;
   while (values.hasNext()) {
      DoubleWritable value = values.next();
      wordCount += value.get();

   }
    output.collect(key,new DoubleWritable(wordCount));
  }
}
