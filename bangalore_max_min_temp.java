package bangalore_max_min_temp ;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class mmtb {

    public static class BangaloreMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private final static Text BANGALORE_KEY = new Text("Bangalore");
        private DoubleWritable tempVal = new DoubleWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            
            // Skip header or empty lines
            if (line.startsWith("Date") || line.isEmpty()) {
                return;
            }

            // Split by space-delimited fields: Date Temp City Country Latitude Longitude
            String[] fields = line.split("\\s+");
            
            if (fields.length >= 3) {
                String cityName = fields[2].trim();
                
                // Filter exclusively for Bangalore (case-insensitive)
                if (cityName.equalsIgnoreCase("Bangalore")) {
                    try {
                        double temp = Double.parseDouble(fields[1].trim());
                        tempVal.set(temp);
                        context.write(BANGALORE_KEY, tempVal);
                    } catch (NumberFormatException e) {
                        // Ignore malformed numeric inputs
                    }
                }
            }
        }
    }

    public static class BangaloreReducer extends Reducer<Text, DoubleWritable, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
                throws IOException, InterruptedException {
            
            double maxTemp = Double.NEGATIVE_INFINITY;
            double minTemp = Double.POSITIVE_INFINITY;
            boolean hasData = false;

            for (DoubleWritable val : values) {
                double currentTemp = val.get();
                if (currentTemp > maxTemp) {
                    maxTemp = currentTemp;
                }
                if (currentTemp < minTemp) {
                    minTemp = currentTemp;
                }
                hasData = true;
            }

            if (hasData) {
                result.set("Max Temp: " + maxTemp + "\tMin Temp: " + minTemp);
                context.write(key, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Bangalore Max and Min Temperature");

        job.setJarByClass(mmtb.class);
        job.setMapperClass(BangaloreMapper.class);
        job.setReducerClass(BangaloreReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}