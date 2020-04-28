import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import java.util.*;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.Math;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.util.concurrent.LinkedBlockingQueue;


public class invertedindex {

    public static class fileNameMapper extends Mapper<LongWritable,Text,wordpair,IntWritable> {
        private wordpair wordPair = new wordpair();
        private IntWritable ONE = new IntWritable(1);
        private Map<String, Integer> map; // global assiciative array


        public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {

            Map<String, Integer> map = getMap();
            StringTokenizer term = new StringTokenizer(value.toString());

            while (term.hasMoreTokens()) {
                String token = term.nextToken();
                token = token.replaceAll("\\W+", "");

                if (map.containsKey(token)) {
                    int total = map.get(token) + 1;
                    map.put(token, total);
                } else {
                    map.put(token, 1);
                }
            }
        }


        protected void cleanup(Context context) throws IOException, InterruptedException {
            Map<String, Integer> map = getMap();
            Iterator<Map.Entry<String, Integer>> it = map.entrySet().iterator();
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

            while(it.hasNext()) {
                Map.Entry<String, Integer> entry = it.next();
                String term = entry.getKey();
                wordPair.setWord(term);
                wordPair.setNeighbor(fileName);
                int total = entry.getValue().intValue();
                //context.write(wordPair, new IntWritable(total));
                context.write(wordPair, new IntWritable(total));
            } //end of while
        } //end of cleanup

        public Map<String, Integer> getMap() {
            if(null == map)
                map = new HashMap<String, Integer>();
            return map;
        } //end of getMap
}

    public static class PairsRelativeOccurrenceReducer extends Reducer<wordpair, IntWritable, Text, Text>
    {
        //Text currTerm= new Text();
        Text prevTerm=new Text();
        String postingList= "";

        protected void reduce(wordpair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable value : values) {

//                System.out.println("ENTERED THE REDUCER!!!");
//                System.out.println("key: "+key.getWord().toString());
//                System.out.println("prevTerm: "+prevTerm);
                boolean matches=key.getWord().equals(prevTerm);

                if((matches==false) && (prevTerm!=null))
                {
                    //System.out.println("posting list: "+postingList);
                    context.write(prevTerm, new Text(postingList));
                    postingList="";
                }

                    String addition= (key.getNeighbor().toString()+ ":"+ value.get()+"; ");
                    postingList+=addition;
                    prevTerm.set(key.getWord());
            }
        }
        protected void cleanup(Context context) throws IOException, InterruptedException
        {
            System.out.println("CLEANUP");
            context.write(prevTerm, new Text(postingList));
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "Pairs");
        job.setJarByClass(invertedindex.class);


        job.setMapperClass(fileNameMapper.class);
        job.setMapOutputKeyClass(wordpair.class);
        job.setMapOutputValueClass(IntWritable.class);


        job.setReducerClass(PairsRelativeOccurrenceReducer.class);
        job.setNumReduceTasks(3);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
