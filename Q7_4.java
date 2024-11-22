Q-7.4 Write a mapreduce job to display only titles of the movie having “Gold” anywhere in the title.

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// Mapper 

public class GoldMoviesMapper extends Mapper<Object, Text, Text, Text> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields.length >= 2 && fields[1].toLowerCase().contains("gold")) {
            context.write(new Text(fields[0]), new Text(fields[1]));
        }
    }
}


// Reducer 

public class GoldMoviesReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text val : values) {
            context.write(key, val);
        }
    }
}

// Driver 

public class GoldMoviesDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Movies with Gold in Title");

        job.setJarByClass(GoldMoviesDriver.class);
        job.setMapperClass(GoldMoviesMapper.class);
        job.setReducerClass(GoldMoviesReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

