package max_min_city_temp;

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

public class mmtc {

    public static class DynamicCityMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private Text cityKey = new Text();
        private DoubleWritable tempVal = new DoubleWritable();
        private String targetCity;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Retrieve the city name passed from the driver via Configuration
            Configuration conf = context.getConfiguration();
            targetCity = conf.get("target.city");
        }

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

                // Match against the city passed dynamically via command line
                if (targetCity != null && cityName.equalsIgnoreCase(targetCity)) {
                    try {
                        double temp = Double.parseDouble(fields[1].trim());
                        cityKey.set(cityName); // Preserve actual dataset casing
                        tempVal.set(temp);
                        context.write(cityKey, tempVal);
                    } catch (NumberFormatException e) {
                        // Ignore malformed temperature entries
                    }
                }
            }
        }
    }

    public static class DynamicCityReducer extends Reducer<Text, DoubleWritable, Text, Text> {
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
        if (args.length < 3) {
            System.err.println("Usage: DynamicCityMaxMinTemp <input path> <output path> <city_name>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        // Pass the 3rd command-line argument (city name) into the Hadoop Configuration
        conf.set("target.city", args[2]);

        Job job = Job.getInstance(conf, "Dynamic City Max and Min Temperature");

        job.setJarByClass(mmtc.class);
        job.setMapperClass(DynamicCityMapper.class);
        job.setReducerClass(DynamicCityReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}