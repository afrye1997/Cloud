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
import org.apache.hadoop.util.GenericOptionsParser;

public class BigramCount {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text currentWord = new Text();
    private Text bigram = new Text();

    //map function
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
      context.write(new Text("~~~~MapCount"), one);
      StringTokenizer itr = new StringTokenizer(value.toString());
      String prevWord = null;

      //Skip line if just 1 word
      if (itr.countTokens() > 1) {
        while (itr.hasMoreTokens()) {
          currentWord.set(itr.nextToken());
          if (prevWord != null)   //restarts on each new value
          {
            bigram.set(prevWord + " " + currentWord);
            context.write(bigram, one);
          }
          prevWord = currentWord.toString();
        }
      }
    }

    //clean up function called at end of each map task
    protected void cleanup(Context context) throws IOException, InterruptedException {
      context.write(new Text("~~~~MapTask Count"), one);
    }


  }
  public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable>
  {
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
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "word count");
    job.setJarByClass(BigramCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }
}
