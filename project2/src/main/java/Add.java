import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputFormat;

class Block implements Writable {
    int rows;
    int columns;
    public double[][] data;

    Block() {}

    Block(int rows, int columns) {
        this.rows = rows;
        this.columns = columns;
        data = new double[rows][columns];
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(rows);
        out.writeInt(columns);
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < columns; j++) {
                out.writeDouble(data[i][j]);
            }
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        rows = in.readInt();
        columns = in.readInt();
        data = new double[rows][columns];
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < columns; j++) {
                data[i][j] = in.readDouble();
            }
        }
    }

    @Override
    public String toString() {
        String s = "\n";
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < columns; j++)
                s += String.format("\t%.3f", data[i][j]);
            s += "\n";
        }
        return s;
    }
}

class Pair implements WritableComparable<Pair> {
    public int i;
    public int j;

    Pair() {}

    Pair(int i, int j) {
        this.i = i;
        this.j = j;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(i);
        out.writeInt(j);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        i = in.readInt();
        j = in.readInt();
    }

    @Override
    public int compareTo(Pair o) {
        int result = Integer.compare(i, o.i);
        if (result != 0) return result;
        return Integer.compare(j, o.j);
    }

    @Override
    public String toString() {
        return "" + i + "\t" + j;
    }
}

class Triple implements Writable {
    public int i;
    public int j;
    public double value;

    Triple() {}

    Triple(int i, int j, double v) {
        this.i = i;
        this.j = j;
        value = v;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(i);
        out.writeInt(j);
        out.writeDouble(value);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        i = in.readInt();
        j = in.readInt();
        value = in.readDouble();
    }
}

public class Add {

    public static class MatrixToBlockMapper extends Mapper<Object, Text, Pair, Triple> {
        private int rows;
        private int columns;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            rows = conf.getInt("rows", 0);
            columns = conf.getInt("columns", 0);
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            int i = Integer.parseInt(parts[0]);
            int j = Integer.parseInt(parts[1]);
            double v = Double.parseDouble(parts[2]);
            Pair blockCoord = new Pair(i / rows, j / columns);
            Triple element = new Triple(i % rows, j % columns, v);
            context.write(blockCoord, element);
        }
    }

    public static class BlockReducer extends Reducer<Pair, Triple, Pair, Block> {
        private int rows;
        private int columns;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            rows = conf.getInt("rows", 0);
            columns = conf.getInt("columns", 0);
        }

        @Override
        public void reduce(Pair key, Iterable<Triple> values, Context context) throws IOException, InterruptedException {
            Block block = new Block(rows, columns);
            for (Triple val : values) {
                block.data[val.i][val.j] = val.value;
            }
            context.write(key, block);
        }
    }

    public static class BlockMapper extends Mapper<Pair, Block, Pair, Block> {
        @Override
        public void map(Pair key, Block value, Context context) throws IOException, InterruptedException {
            // Directly emit the input key and value without any processing
            context.write(key, value);
        }
    }

    public static class BlockAdditionReducer extends Reducer<Pair, Block, Pair, Block> {
        @Override
        public void reduce(Pair key, Iterable<Block> values, Context context) throws IOException, InterruptedException {
            Iterator<Block> iter = values.iterator();
            Block result = new Block(context.getConfiguration().getInt("rows", 0), context.getConfiguration().getInt("columns", 0));

            while (iter.hasNext()) {
                Block nextBlock = iter.next();
                for (int i = 0; i < result.rows; i++) {
                    for (int j = 0; j < result.columns; j++) {
                        result.data[i][j] += nextBlock.data[i][j];
                    }
                }
            }
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInt("rows", Integer.parseInt(args[0]));
        conf.setInt("columns", Integer.parseInt(args[1]));

        // Job 1: Convert Matrix M to Block Matrix
        Job job1 = Job.getInstance(conf, "Convert Matrix M to Block Matrix");
        configureMatrixToBlockJob(job1, args[2], args[4], MatrixToBlockMapper.class, BlockReducer.class);

        // Job 2: Convert Matrix N to Block Matrix
        Job job2 = Job.getInstance(conf, "Convert Matrix N to Block Matrix");
        configureMatrixToBlockJob(job2, args[3], args[5], MatrixToBlockMapper.class, BlockReducer.class);

        // Job 3: Block Matrix Addition
        Job job3 = Job.getInstance(conf, "Block Matrix Addition");
        job3.setJarByClass(Add.class);
        job3.setReducerClass(BlockAdditionReducer.class);
        job3.setOutputKeyClass(Pair.class);
        job3.setOutputValueClass(Block.class);
        job3.setInputFormatClass(SequenceFileInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        
        MultipleInputs.addInputPath(job3, new Path(args[4]), SequenceFileInputFormat.class, BlockMapper.class);
        MultipleInputs.addInputPath(job3, new Path(args[5]), SequenceFileInputFormat.class, BlockMapper.class);
        FileOutputFormat.setOutputPath(job3, new Path(args[6]));

        // Execute the jobs sequentially
        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }
        if (!job2.waitForCompletion(true)) {
            System.exit(1);
        }
        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }

    private static void configureMatrixToBlockJob(Job job, String inputPath, String outputPath, 
                                                   Class<? extends Mapper> mapperClass, 
                                                   Class<? extends Reducer> reducerClass) throws IOException {
        job.setJarByClass(Add.class);
        job.setMapperClass(mapperClass);
        job.setReducerClass(reducerClass);
        job.setMapOutputKeyClass(Pair.class);
        job.setMapOutputValueClass(Triple.class);
        job.setOutputKeyClass(Pair.class);
        job.setOutputValueClass(Block.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
    }
}
