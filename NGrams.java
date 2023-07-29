// Input at gs://coc105-gutenburg-10000books/
// Output at gs://for-cluster/output-final/

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;

public class NGrams 
{
    public static class NGramMapper extends Mapper<Object, Text, Text, IntWritable> 
    {
        private final static IntWritable one = new IntWritable(1);
        private String word = null, word2 = null, word3 = null, word4 = null; // 4-grams implementation
        private Text ngram = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
        {
            StringTokenizer itr = new StringTokenizer(value.toString().replaceAll("\\p{Punct}",""));
            while (itr.hasMoreTokens()) 
            {
                word4 = itr.nextToken();
                if(word != null) {
                    ngram.set(word + " " + word2 + " " + word3 + " " + word4); // Creating an engram
                    context.write(ngram, one);
                }
                // Shifts words for the next ngram
                word = word2;
                word2 = word3;
                word3 = word4;
            }
        }
    }
    public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> 
    {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
        {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
    public static class SomePartitioner extends Partitioner<Text, IntWritable> 
    {
        @Override
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            
            // Partitioner partitions based on the starting character in ASCII format
            char keyValue = key.toString().toLowerCase().charAt(0);
            int ascii = keyValue;

            if (ascii > 47 && ascii < 58) { // 0 to 9
                return 1 % numReduceTasks;
            }
            if (ascii > 96 && ascii < 104) { // a to g
                return 2 % numReduceTasks;
            }
            if (ascii > 103 && ascii < 110) { // h to m
                return 3 % numReduceTasks;
            }
            if (ascii > 109 && ascii < 116) { // n to s
                return 4 % numReduceTasks;
            }
            if (ascii > 115 && ascii < 123) { // t to z
                return 5 % numReduceTasks;
            }
            else { // any other char
                return 0;
            }
        }
    }
    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();

        // Cluster properties below (For when creating a cluster):
        // Compress intermediate data with Snappy
        conf.set("mapreduce.map.output.compress", "true");
        conf.set("mapreduce.map.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
        // Enable speculative execution
        conf.set("mapreduce.map.speculative", "true");
        conf.set("mapreduce.reduce.speculative", "true");
        // Increase slow start
        conf.set("mapreduce.job.reduce.slowstart.completedmaps", "0.9");
        // Increase buffer memory
        conf.set("mapreduce.task.io.sort.mb", "1024");
        // Increase memory allocated
        conf.set("mapreduce.map.memory.mb", "2048");
        conf.set("mapreduce.reduce.memory.mb", "4096");
        // Split max size for fileinputformat
        conf.set("mapreduce.input.fileinputformat.split.maxsize", "134217728");
        // End of cluster properties
        
        Job job = Job.getInstance(conf, "ngrams");
        job.setInputFormatClass(CombineTextInputFormat.class); // Combine input files
        job.setJarByClass(NGrams.class);
        job.setMapperClass(NGramMapper.class);
        job.setPartitionerClass(SomePartitioner.class); // Set Partitioner
        job.setCombinerClass(NGramReducer.class); // Set Combiner to reduce intermediate data from mapper to reducer
        job.setReducerClass(NGramReducer.class);
        job.setNumReduceTasks(6); // Set number of reducer tasks
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}