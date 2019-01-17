import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class App {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(), "Airports traffic");
        job.setJarByClass(App.class);

        FileUtils.deleteDirectory(new File("src/main/resources/results"));  // Delete the last result folder

        FileInputFormat.addInputPath(job, new Path("src/main/resources/traffic1hour.exp2"));
        FileOutputFormat.setOutputPath(job, new Path("src/main/resources/results"));

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] subvalues = value.toString().split(";");   // Format: origin;destination;
            Text key1 = new Text(subvalues[0]);                        // Origin
            Text key2 = new Text(subvalues[1]);                        // Destination
            IntWritable value1 = new IntWritable(1);            // New Flight
            context.write(key1, value1);
            context.write(key2, value1);
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int res = 0;
            for (IntWritable flight : values) {
                res = res + 1;          // Sum of flights in each airport
            }
            context.write(key, new IntWritable(res));
        }
    }
}
