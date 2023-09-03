import java.io.*;
import java.util.Scanner;

//import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Netflix {
    /* ... */
    public static class MyMapper extends Mapper<Object,Text,IntWritable,IntWritable> {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            if (!(value.toString().endsWith(":"))){
                Scanner s = new Scanner(value.toString()).useDelimiter(",");
                int user = s.nextInt();
                int rating = s.nextInt();
                context.write(new IntWritable(user),new IntWritable(rating));
                s.close();
            }
        }
    }

    public static class MyReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
        @Override
        public void reduce ( IntWritable user, Iterable<IntWritable> values, Context context )
                           throws IOException, InterruptedException {
            double sum = 0.0;
            double count = 0.0;
            for (IntWritable v: values) {
                sum += v.get();
                count++;
            };
            context.write(user,new IntWritable((int)((sum/count)*10)));
        }
    }

    public static class MyMapper2 extends Mapper<Object,Text,IntWritable,IntWritable> {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            
            Scanner s = new Scanner(value.toString()).useDelimiter("\t");
            int user = s.nextInt();
            int rating = s.nextInt();
            context.write(new IntWritable(rating),new IntWritable(1));
            s.close();
        }
    }

    public static class MyReducer2 extends Reducer<IntWritable,IntWritable,DoubleWritable,IntWritable> {
        @Override
        public void reduce ( IntWritable rating, Iterable<IntWritable> values, Context context )
                           throws IOException, InterruptedException {
            long sum = 0;
            // long count = 0;
            for (IntWritable v: values) {
                sum += v.get();
                // count++;
            };
            context.write(new DoubleWritable((double)(rating.get())/10.0),new IntWritable((int)(sum)));
        }
    }

    public static void main ( String[] args ) throws Exception {
        /* ... */
        Job job = Job.getInstance();
        job.setJobName("MyJob");
        job.setJarByClass(Netflix.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        job.waitForCompletion(true);
        Job job2 = Job.getInstance();
        job2.setJobName("MyJob2");
        job2.setJarByClass(Netflix.class);
        job2.setOutputKeyClass(DoubleWritable.class);
        job2.setOutputValueClass(IntWritable.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setMapperClass(MyMapper2.class);
        job2.setReducerClass(MyReducer2.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job2,new Path(args[1]));
        FileOutputFormat.setOutputPath(job2,new Path(args[2]));
        job2.waitForCompletion(true);
    }
}
