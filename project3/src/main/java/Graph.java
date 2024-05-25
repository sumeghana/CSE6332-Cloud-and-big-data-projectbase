import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat; // Add this import


class Vertex implements Writable {
    public short tag; // 0 for a graph vertex, 1 for a group number
    public long group;
    public long VID;
    public long[] adjacent;

    Vertex() {}

    Vertex(short tag, long group, long VID, long[] adjacent) {
        this.tag = tag;
        this.group = group;
        this.VID = VID;
        this.adjacent = adjacent;
    }

    Vertex(short tag, long group) {
        this.tag = tag;
        this.group = group;
        this.VID = 0;
        this.adjacent = new long[0];
    }

    public void write(DataOutput out) throws IOException {
        out.writeShort(tag);
        out.writeLong(group);
        out.writeLong(VID);
        out.writeInt(adjacent.length);
        for (long a : adjacent) out.writeLong(a);
    }

    public void readFields(DataInput in) throws IOException {
        tag = in.readShort();
        group = in.readLong();
        VID = in.readLong();
        int size = in.readInt();
        adjacent = new long[size];
        for (int i = 0; i < size; i++) adjacent[i] = in.readLong();
    }
}

public class Graph {

    public static class FirstMapper extends Mapper<Object, Text, LongWritable, Vertex> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), ",");
            long VID = Long.parseLong(itr.nextToken());
            long[] adjacent = new long[itr.countTokens()];
            for (int i = 0; itr.hasMoreTokens(); i++) {
                adjacent[i] = Long.parseLong(itr.nextToken());
            }
            context.write(new LongWritable(VID), new Vertex((short) 0, VID, VID, adjacent));
        }
    }

    public static class FirstReducer extends Reducer<LongWritable, Vertex, LongWritable, Vertex> {
        @Override
        public void reduce(LongWritable key, Iterable<Vertex> values, Context context) throws IOException, InterruptedException {
            for (Vertex v : values) {
                context.write(new LongWritable(v.VID), v);
            }
        }
    }

    public static class SecondMapper extends Mapper<LongWritable, Vertex, LongWritable, Vertex> {
        @Override
        public void map(LongWritable key, Vertex value, Context context) throws IOException, InterruptedException {
            context.write(new LongWritable(value.VID), value); // Emit the vertex itself
            for (long n : value.adjacent) {
                // Emit group numbers to adjacent vertices
                context.write(new LongWritable(n), new Vertex((short) 1, value.group));
            }
        }
    }

    public static class SecondReducer extends Reducer<LongWritable, Vertex, LongWritable, Vertex> {
        @Override
        public void reduce(LongWritable key, Iterable<Vertex> values, Context context) throws IOException, InterruptedException {
            long m = Long.MAX_VALUE;
            long[] adj = new long[0];
            for (Vertex v : values) {
                if (v.tag == 0) {
                    adj = v.adjacent.clone();
                }
                m = Math.min(m, v.group);
            }
            context.write(new LongWritable(m), new Vertex((short) 0, m, key.get(), adj));
        }
    }

    public static class FinalMapper extends Mapper<LongWritable, Vertex, LongWritable, IntWritable> {
        @Override
        public void map(LongWritable key, Vertex value, Context context) throws IOException, InterruptedException {
            context.write(new LongWritable(value.group), new IntWritable(1));
        }
    }

    public static class FinalReducer extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {
        @Override
        public void reduce(LongWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Graph Processing - Phase 1");
        job1.setJarByClass(Graph.class);
        job1.setMapperClass(FirstMapper.class);
        job1.setReducerClass(FirstReducer.class);
        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(Vertex.class);
        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(Vertex.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/f0"));
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        job1.waitForCompletion(true);

        for (int i = 0; i < 5; i++) {
            Job job2 = Job.getInstance(conf, "Graph Processing - Phase 2 - Iteration " + i);
            job2.setJarByClass(Graph.class);
            job2.setMapperClass(SecondMapper.class);
            job2.setReducerClass(SecondReducer.class);
            job2.setMapOutputKeyClass(LongWritable.class);
            job2.setMapOutputValueClass(Vertex.class);
            job2.setOutputKeyClass(LongWritable.class);
            job2.setOutputValueClass(Vertex.class);
            SequenceFileInputFormat.setInputPaths(job2, new Path(args[1] + "/f" + i));
            SequenceFileOutputFormat.setOutputPath(job2, new Path(args[1] + "/f" + (i + 1)));
            job2.setInputFormatClass(SequenceFileInputFormat.class);
            job2.setOutputFormatClass(SequenceFileOutputFormat.class);
            job2.waitForCompletion(true);
        }

        Job job3 = Job.getInstance(conf, "Graph Processing - Final Phase");
        job3.setJarByClass(Graph.class);
        job3.setMapperClass(FinalMapper.class);
        job3.setReducerClass(FinalReducer.class);
        job3.setMapOutputKeyClass(LongWritable.class);
        job3.setMapOutputValueClass(IntWritable.class);
        job3.setOutputKeyClass(LongWritable.class);
        job3.setOutputValueClass(IntWritable.class);
        SequenceFileInputFormat.setInputPaths(job3, new Path(args[1] + "/f5"));
        FileOutputFormat.setOutputPath(job3, new Path(args[2]));
        job3.setInputFormatClass(SequenceFileInputFormat.class);
        job3.waitForCompletion(true);
    }
}
