Q-7.5 Write a mapreduce that will display the count of the movies which belongs to both Drama and Romantic genre. 

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// Mapper 

public class DramaRomanceMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static Text countKey = new Text("DramaRomance");
    private final static IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields.length >= 3 && fields[2].contains("Drama") && fields[2].contains("Romance")) {
            context.write(countKey, one);
        }
    }
}

// Reducer 

public class DramaRomanceReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));
    }
}

// Driver 

public class DramaRomanceDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Drama and Romance Movies");

        job.setJarByClass(DramaRomanceDriver.class);
        job.setMapperClass(DramaRomanceMapper.class);
        job.setReducerClass(DramaRomanceReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
