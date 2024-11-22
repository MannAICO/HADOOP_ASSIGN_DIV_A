Q-7.2 Write a mapreduce job to find the count of the Documentary movies released in the year 1995.

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// Mapper 

public class Documentary1995Mapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static Text countKey = new Text("Documentary_1995");
    private final static IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields.length >= 3 && fields[2].contains("Documentary") && fields[1].contains("1995")) {
            context.write(countKey, one);
        }
    }
}

// Reducer

public class Documentary1995Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));
    }
}

// Driver 

public class Documentary1995Driver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Documentary Movies in 1995");

        job.setJarByClass(Documentary1995Driver.class);
        job.setMapperClass(Documentary1995Mapper.class);
        job.setReducerClass(Documentary1995Reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
