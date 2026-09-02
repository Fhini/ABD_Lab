package boy_girl_count;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class bg {
    public static class testWordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text genderKey = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            
            // Skip the header row if present in the dataset
            if (line.startsWith("Student Name")) {
                return;
            }

            // Split the comma-separated values
            String[] fields = line.split(",");
            
            // Ensure the line has the expected format (Gender is at index 3)
            if (fields.length >= 4) {
                String gender = fields[3].trim().toLowerCase();
                
                // Map Male/Female entries to Boy/Girl categories as requested
                if (gender.equals("male") || gender.equals("boy")) {
                    genderKey.set("Boy");
                } else if (gender.equals("female") || gender.equals("girl")) {
                    genderKey.set("Girl");
                } else {
                    genderKey.set(fields[3].trim()); // Fallback for any other values
                }
                
                context.write(genderKey, one);
            }
        }
    }
    
    public static class testWordCountReducer extends Reducer <Text, IntWritable, Text, IntWritable > {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) 
            throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable x: values) {
                sum += x.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
        Job job = Job.getInstance(conf, "gender student count");

        job.setJarByClass(bg.class);
        job.setMapperClass(testWordCountMapper.class);
        job.setReducerClass(testWordCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}