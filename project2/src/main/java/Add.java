import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Triple implements Writable {
    public int i;
    public int j;
    public double value;
	
    Triple () {}
	
    Triple ( int i, int j, double v ) { this.i = i; this.j = j; this.value = v; }
	
    public void write ( DataOutput out ) throws IOException {
	    out.writeInt(this.i);
	    out.writeInt(this.j);
	    out.writeDouble(this.value);
    }

    public void readFields ( DataInput in ) throws IOException {
	    this.i = in.readInt();
	    this.j = in.readInt();
	    this.value = in.readDouble();
    }

    @Override
    public String toString () {
        return ""+i+"\t"+j+"\t"+value;
    }
}

class Block implements Writable {
    final static int rows = 100;
    final static int columns = 100;
    public double[][] data;

    Block () {
        data = new double[rows][columns];
    }

    public void write ( DataOutput out ) throws IOException {
	    for (int i = 0; i < this.rows; i++){
	    	for (int j = 0; j < this.columns; j++){
			out.writeDouble(this.data[i][j]);
		}
	    }
    }

    public void readFields ( DataInput in ) throws IOException {
	    for (int i = 0; i < this.rows; i++){
	    	for (int j = 0; j < this.columns; j++){
			this.data[i][j] = in.readDouble();
		}
	    }
    }

    @Override
    public String toString () {
        String s = "\n";
        for ( int i = 0; i < rows; i++ ) {
            for ( int j = 0; j < columns; j++ )
                s += String.format("\t%.3f",data[i][j]);
            s += "\n";
        }
        return s;
    }
}

class Pair implements WritableComparable<Pair> {
    public int i;
    public int j;
	
    Pair () {}
	
    Pair ( int i, int j ) { this.i = i; this.j = j; }
	
    public void write ( DataOutput out ) throws IOException {
	    out.writeInt(this.i);
	    out.writeInt(this.j);
    }

    public void readFields ( DataInput in ) throws IOException {
	    this.i = in.readInt();
	    this.j = in.readInt();
    }

    @Override
    public int compareTo ( Pair o ) {
	    if (this.i == o.i){
	    	return this.j - o.j;
	    } 
	    else{
	    	return this.i - o.i;
	    }
    }

    @Override
    public String toString () {
        return ""+i+"\t"+j;
    }
}

public class Add extends Configured implements Tool {
    final static int rows = Block.rows;
    final static int columns = Block.columns;

    /* ... */

    public static class PairTripleMapper extends Mapper<Object,Text,Pair,Triple>{
    	@Override
    	public void map(Object key, Text line, Context context) throws IOException, InterruptedException{
    		Scanner s = new Scanner(line.toString()).useDelimiter(",");
		int i = s.nextInt();
		int j = s.nextInt();
		double value = s.nextDouble();
		context.write(new Pair(i/rows, j/columns), new Triple(i%rows, j%columns, value));
		s.close();
    	}
    }

    public static class PairTripleReducer extends Reducer<Pair,Triple, Pair,Block>{
	
	@Override
	public void reduce(Pair key, Iterable<Triple> values, Context context) 
		throws IOException, InterruptedException{
		Block b = new Block();
		for (Triple t: values){
			int i = t.i;
			int j = t.j;
			double v = t.value;
			b.data[i][j] = v;
		}
		context.write(key, b);
	}
    }


    public static class AdditionMap1 extends Mapper<Pair, Block, Pair, Block>{
    	@Override
	public void map(Pair key, Block b, Context context) 
		throws IOException, InterruptedException{
		context.write(key, b);
	}
    }

    public static class AdditionMap2 extends Mapper<Pair, Block, Pair, Block>{
    	@Override
	public void map(Pair key, Block b, Context context) 
		throws IOException, InterruptedException{
		context.write(key, b);
	}
    }

    public static class AdditionReducer extends Reducer<Pair, Block, Pair, Block>{

	@Override
	public void reduce(Pair key, Iterable<Block> b, Context context) 
		throws IOException, InterruptedException{
		Block sum = new Block();
		for(Block each_block: b){
			for(int i=0; i <rows; i++){
				for(int j=0; j<columns; j++){
					sum.data[i][j]+=each_block.data[i][j];
				}
			}
		}
		context.write(key, sum);
	}
    }
    @Override
    public int run ( String[] args ) throws Exception {
	Job job1 = Job.getInstance();
        job1.setJobName("PairTripleJob1");
        job1.setJarByClass(Add.class);
        job1.setOutputKeyClass(Pair.class);
        job1.setOutputValueClass(Block.class);
        job1.setMapOutputKeyClass(Pair.class);
        job1.setMapOutputValueClass(Triple.class);
        job1.setMapperClass(PairTripleMapper.class);
        job1.setReducerClass(PairTripleReducer.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));
        job1.waitForCompletion(true);

	Job job2 = Job.getInstance();
        job2.setJobName("PairTripleJob2");
        job2.setJarByClass(Add.class);
        job2.setOutputKeyClass(Pair.class);
        job2.setOutputValueClass(Block.class);
        job2.setMapOutputKeyClass(Pair.class);
        job2.setMapOutputValueClass(Triple.class);
        job2.setMapperClass(PairTripleMapper.class);
        job2.setReducerClass(PairTripleReducer.class);
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2,new Path(args[3]));
        job2.waitForCompletion(true);	
	
	Job job3 = Job.getInstance();
        job3.setJobName("AddJob");
        job3.setJarByClass(Add.class);
        job3.setOutputKeyClass(Pair.class);
        job3.setOutputValueClass(Block.class);
        job3.setMapOutputKeyClass(Pair.class);
        job3.setMapOutputValueClass(Block.class);
        //job3.setMapperClass(AdditionMap1.class);
        job3.setReducerClass(AdditionReducer.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        //job3.setMapperClass(AdditionMap1.class);
        MultipleInputs.addInputPath(job3,new Path(args[2]), SequenceFileInputFormat.class, AdditionMap1.class);
        //job3.setMapperClass(AdditionMap2.class);
        MultipleInputs.addInputPath(job3,new Path(args[3]), SequenceFileInputFormat.class, AdditionMap2.class);
        FileOutputFormat.setOutputPath(job3,new Path(args[4]));
        job3.waitForCompletion(true);
 
        return 0;
    }

    public static void main ( String[] args ) throws Exception {
    	ToolRunner.run(new Configuration(),new Add(),args);
    }
}
