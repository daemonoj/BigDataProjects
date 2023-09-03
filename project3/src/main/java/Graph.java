import java.io.*;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Vertex implements Writable {
    public short tag;           // 0 for a graph vertex, 1 for a group number
    public long group;          // the group where this vertex belongs to
    public long vID;            // the vertex ID
    public long[] adjacent;     // the vertex neighbors
    

    Vertex (){}

    Vertex(short tag, long group, long vID, long[] adjacent) {
    	this.tag = tag;
		this.group = group;
		this.vID = vID;
		this.adjacent = new long[adjacent.length];
		for (int i = 0; i < adjacent.length; i++) {
			this.adjacent[i] = adjacent[i];
		}
    }
    Vertex(short tag, long group) {
    	this.tag = tag;
		this.group = group;
    }

    public void write (DataOutput out) throws IOException {
    	StringBuilder sb = new StringBuilder();	
		String result;
    	sb.append(this.tag).append(",");
    	sb.append(this.group).append(",");
    	if (this.tag == 0)
    	{
			sb.append(this.vID).append(",");
			if (this.adjacent.length > 0) {
				for(int i=0; i < this.adjacent.length; i++) {
					sb.append(adjacent[i]).append(",");
				}
    		}
    	}
    	result = sb.deleteCharAt(sb.length() - 1).toString();
    	System.out.println(result);
    	out.writeUTF(result);
    }

    public void readFields (DataInput in) throws IOException {
    	// System.out.println("Inside Read Fields");
    	String [] line = in.readUTF().split(",");
    	this.tag = Short.parseShort(line[0]);
    	this.group = Long.parseLong(line[1]);
    	if (this.tag == 0){
    		// System.out.println(in.readLong());
    		System.out.println("Tag=" + this.tag);
    		System.out.println("Group=" + this.group);
    		
    		this.vID = Long.parseLong(line[2]);

    		System.out.println("VID = "+ this.vID);
    		
    		this.adjacent = new long[line.length - 3];
    		for (int i = 0; i< line.length - 3; i++){
    			this.adjacent[i] = Long.parseLong(line[i + 3]);
    		}
    		for (int i = 0; i < line.length - 3; i++){
    			System.out.println("Value"+i+"= "+this.adjacent[i]);
    		}
    	}
    }
    @Override
    public String toString(){
    	StringBuilder sb = new StringBuilder();	
		String result;
    	sb.append(this.tag).append(",");
    	sb.append(this.group).append(",");
    	if (this.tag == 0)
    	{
			sb.append(this.vID).append(",");
			if (this.adjacent.length > 0) {
				for(int i=0; i < this.adjacent.length; i++) {
					sb.append(adjacent[i]).append(",");
				}
    		}
    	}
    	result = sb.deleteCharAt(sb.length() - 1).toString();
    	return result;
    }
}

public class Graph {

    /* ... */
    
    public static class VIDMapper extends Mapper<Object, Text, LongWritable, Vertex> {
//    	@Override
		public void map (Object key, Text value, Context context) throws IOException, InterruptedException {
		    Scanner s = new Scanner(value.toString());
		    String [] line = s.nextLine().split(",");
		    long vid = Long.parseLong(line[0]);
		    long [] adj = new long[line.length - 1];
		    for (int i = 1; i<line.length; i++) {
		    	adj[i-1] = Long.parseLong(line[i]);
		    }
		    Vertex v = new Vertex((short)(0), vid, vid, adj);
		    context.write(new LongWritable(vid), v);
		}
    }
    
    
    public static class GroupMapper extends Mapper<LongWritable, Vertex, LongWritable, Vertex> {
	
    	public void map (LongWritable key, Vertex v, Context context) throws IOException, InterruptedException {
    		// System.out.println(v.tag);
    		// System.out.println(v.group);
    		System.out.println("GroupMapper");
    		context.write(new LongWritable(v.vID), v);
    		System.out.println(v.adjacent.length);
    		for (int i = 0; i< v.adjacent.length; i++){
    			context.write(new LongWritable(v.adjacent[i]), new Vertex((short)(1), v.group));
    		}
    	}
    }
    public static class GroupReducer extends Reducer<LongWritable, Vertex, LongWritable, Vertex> {
	
    	public void reduce (LongWritable vid, Iterable<Vertex> values, Context context) throws IOException, InterruptedException {
   			long m = Long.MAX_VALUE;
			long [] adj = new long[0];
    		for (Vertex v: values) {
    			if (v.tag == 0) {
    				adj = new long[v.adjacent.length];
    				System.arraycopy(v.adjacent, 0 , adj, 0, v.adjacent.length);
    			}
				m = Math.min(m, v.group);
    		}
    		context.write(new LongWritable(m), new Vertex((short)0, m, vid.get(), adj));
    	}
    }
    public static class FinalMap extends Mapper<LongWritable, Vertex, LongWritable, IntWritable> {
		// @Override
    	public void map (LongWritable group, Vertex v, Context context) throws IOException, InterruptedException {
    		System.out.println("Reading the files");
    		context.write(group, new IntWritable(1));
    	}
    }
    
    public static class FinalReduce extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {
		// @Override
    	public void reduce (LongWritable group, Iterable<IntWritable> values, Context context) 
    					throws IOException, InterruptedException {
    		int m = 0;
    		for (IntWritable v : values) {
    			m = m + v.get();
    		}
    		context.write(group, new IntWritable(m));		
    	}
    }
    public static void main ( String[] args ) throws Exception {
        
        Job job1 = Job.getInstance();
        job1.setJobName("J1");
        job1.setJarByClass(Graph.class);
        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(Vertex.class);
        job1.setNumReduceTasks(0);
        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(Vertex.class);
        job1.setMapperClass(VIDMapper.class);
        //job1.setReducerClass(VIDReducer.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/f0"));
        job1.waitForCompletion(true);
        /* ... First Map-Reduce job to read the graph */
        
        Job job2;
     //    job2 = Job.getInstance();
     //    job2.setJobName("J2");
    	// job2.setJarByClass(Graph.class);
    	// job2.setOutputKeyClass(LongWritable.class);
    	// job2.setOutputValueClass(Vertex.class);
	    // job2.setMapOutputKeyClass(LongWritable.class);
	    // job2.setMapOutputValueClass(Vertex.class);
	    // job2.setMapperClass(GroupMapper.class);
	    // job2.setReducerClass(GroupReducer.class);
	    // job2.setOutputFormatClass(SequenceFileOutputFormat.class);
	    // job2.setInputFormatClass(SequenceFileInputFormat.class);
	    // FileInputFormat.setInputPaths(job2, new Path(args[1] + "/f0"));
	    // FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/f1"));
	    // job2.waitForCompletion(true);

	    // job2 = Job.getInstance();
     //    job2.setJobName("J3");
    	// job2.setJarByClass(Graph.class);
    	// job2.setOutputKeyClass(LongWritable.class);
    	// job2.setOutputValueClass(Vertex.class);
	    // job2.setMapOutputKeyClass(LongWritable.class);
	    // job2.setMapOutputValueClass(Vertex.class);
	    // job2.setMapperClass(GroupMapper.class);
	    // job2.setReducerClass(GroupReducer.class);
	    // job2.setOutputFormatClass(TextOutputFormat.class);
	    // job2.setInputFormatClass(SequenceFileInputFormat.class);
	    // FileInputFormat.setInputPaths(job2, new Path(args[1] + "/f1"));
	    // FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/f2"));
	    // job2.waitForCompletion(true);

        for ( short i = 0; i < 5; i++ ) {
            job2 = Job.getInstance();
            job2.setJobName("J2");
	    	job2.setJarByClass(Graph.class);
   	    	job2.setOutputKeyClass(LongWritable.class);
	    	job2.setOutputValueClass(Vertex.class);
		    job2.setMapOutputKeyClass(LongWritable.class);
		    job2.setMapOutputValueClass(Vertex.class);
		    job2.setMapperClass(GroupMapper.class);
		    job2.setReducerClass(GroupReducer.class);
		    job2.setOutputFormatClass(SequenceFileOutputFormat.class);
		    job2.setInputFormatClass(SequenceFileInputFormat.class);
		    FileInputFormat.setInputPaths(job2, new Path(args[1] + "/f" + i));
		    FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/f" + (i+1)));
		    job2.waitForCompletion(true);
            /* ... Second Map-Reduce job to propagate the group number */
            // job.waitForCompletion(true);
        }

        System.out.println("Starting Job 3");
        Job job3 = Job.getInstance();
        job3.setJobName("J3");
        job3.setJarByClass(Graph.class);
        job3.setOutputKeyClass(LongWritable.class);
        job3.setOutputValueClass(IntWritable.class);
        job3.setMapOutputKeyClass(LongWritable.class);
        job3.setMapOutputValueClass(IntWritable.class);
        job3.setMapperClass(FinalMap.class);
        job3.setReducerClass(FinalReduce.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        job3.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.setInputPaths(job3, new Path(args[1] + "/f5"));
        FileOutputFormat.setOutputPath(job3, new Path(args[2]));
        /* ... Final Map-Reduce job to calculate the connected component sizes */
        job3.waitForCompletion(true);
    }
}
