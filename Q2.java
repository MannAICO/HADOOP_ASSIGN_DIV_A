Q-2  Analyze the minimum temperature for each year.

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


//Maper

public class MinTemperatureMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text year = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(",");
        year.set(parts[0]); // Assuming year is the first field
        int temperature = Integer.parseInt(parts[1]);
        context.write(year, new IntWritable(temperature));
    }
}

// Reducer

public class MinTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int minTemp = Integer.MAX_VALUE;
        for (IntWritable val : values) {
            minTemp = Math.min(minTemp, val.get());
        }
        context.write(key, new IntWritable(minTemp));
    }
}


// Driver 

public class MinTemperatureDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Minimum Temperature");

        job.setJarByClass(MinTemperatureDriver.class);
        job.setMapperClass(MinTemperatureMapper.class);
        job.setReducerClass(MinTemperatureReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
