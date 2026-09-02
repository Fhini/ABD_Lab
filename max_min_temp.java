package max_min_temp;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class mmt {

    public static class TempMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text city = new Text();
        private Text tempText = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            
            // Skip header if present
            if (line.startsWith("Date") || line.isEmpty()) {
                return;
            }

            // Split by space as the dataset uses space-separated values
            String[] fields = line.split("\\s+");
            
            // Expected format: Date Temp City Country Latitude Longitude
            if (fields.length >= 3) {
                try {
                    String cityName = fields[2].trim();
                    String temp = fields[1].trim();
                    
                    // Validate temperature format
                    Double.parseDouble(temp);
                    
                    city.set(cityName);
                    tempText.set(temp);
                    context.write(city, tempText);
                } catch (NumberFormatException e) {
                    // Skip invalid temperature records
                }
            }
        }
    }

    public static class TempReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            
            double maxTemp = Double.MIN_VALUE;
            double minTemp = Double.MAX_VALUE;

            for (Text val : values) {
                double currentTemp = Double.parseDouble(val.toString());
                if (currentTemp > maxTemp) {
                    maxTemp = currentTemp;
                }
                if (currentTemp < minTemp) {
                    minTemp = currentTemp;
                }
            }

            result.set("Max Temp: " + maxTemp + "\tMin Temp: " + minTemp);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
        Job job = Job.getInstance(conf, "Max and Min City Temperature");

        job.setJarByClass(mmt.class);
        job.setMapperClass(TempMapper.class);
        job.setReducerClass(TempReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}