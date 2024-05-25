import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Trip {

    public static class EfficientTripMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {

        private final IntWritable outDistance = new IntWritable();
        private final DoubleWritable outAmount = new DoubleWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (!line.isEmpty() && Character.isDigit(line.charAt(0))) {
                String[] parts = line.split(",");
                if (parts.length > 5) {
                    try {
                        double rawDistance = Double.parseDouble(parts[4]);
                        if (rawDistance >= 0) { // Ensure distance is non-negative
                            int distance = (int) Math.round(rawDistance);
                            double amount = Double.parseDouble(parts[parts.length - 1]);
                            if (distance < 200 && distance >= 0) { // Check for valid distance range
                                outDistance.set(distance);
                                outAmount.set(amount);
                                context.write(outDistance, outAmount);
                            }
                        }
                    } catch (NumberFormatException e) {
                        // Ignore parsing errors
                    }
                }
            }
        }
    }

    public static class EfficientTripReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {

        private final DoubleWritable result = new DoubleWritable();

        @Override
        protected void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }
            if (count > 0) {
                double average = sum / count;
                average = Math.floor(average * 1e6) / 1e6; // Round the average to 6 decimal places
                result.set(average);
                context.write(key, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Efficient Trip Distance Analysis");
        job.setJarByClass(Trip.class);
        job.setMapperClass(EfficientTripMapper.class);
        job.setCombinerClass(EfficientTripReducer.class);
        job.setReducerClass(EfficientTripReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
