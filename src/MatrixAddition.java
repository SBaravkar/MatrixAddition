import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MatrixAddition {

public static class MatrixAdditionMapper extends Mapper < LongWritable, Text, Text, IntWritable > {
private final static IntWritable outputValue = new IntWritable(); 
private Text outputKey = new Text();

@Override 
public void map( LongWritable key, Text value, Context context) throws IOException, InterruptedException { 
	String[] tokens = value.toString().split(" ");
    //String matrixName = tokens[0];
    int rowIndex = Integer.parseInt(tokens[1]);
    int colIndex = Integer.parseInt(tokens[2]);
    int matrixValue = Integer.parseInt(tokens[3]);
    
    outputKey.set(rowIndex + "," + colIndex);
    outputValue.set(matrixValue);
    context.write(outputKey, outputValue);
} 
public static class MatrixAdditionReducer extends Reducer < Text, IntWritable, Text, IntWritable > { 
private IntWritable result = new IntWritable(); 
@Override 
public void reduce( Text key, Iterable < IntWritable > values, Context context) throws IOException, InterruptedException { 
int sum = 0; 
for (IntWritable val : values) { 
sum += val.get(); 
} 
result.set( sum); 
context.write( key, result); 
} 
}

public static void main( String[] args) throws Exception { 
Configuration conf = new Configuration();
Job job = Job.getInstance( conf, "MatrixAddition");

job.setJarByClass(MatrixAddition.class);

FileInputFormat.addInputPath( job, new Path("input")); 
FileOutputFormat.setOutputPath( job, new Path("output")); 
job.setMapperClass(MatrixAdditionMapper.class); 
job.setCombinerClass(MatrixAdditionReducer.class); 
job.setReducerClass( MatrixAdditionReducer.class);

job.setOutputKeyClass(Text.class); 
job.setOutputValueClass(IntWritable.class);

System.exit( job.waitForCompletion( true) ? 0 : 1); 
} 
}
}