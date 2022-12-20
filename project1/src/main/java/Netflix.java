import java.io.*;
import java.util.Scanner;
import java.math.RoundingMode;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Netflix {
    //First Mapper Starts
//Pseudo code
//map ( key, line ):
//if the line doesn't end with ":"
//read 2 integers from the line into the variables user and rating (delimiter is comma ",")
//emit( user, rating )
	public static class Mapper1 extends Mapper<Object, Text, IntWritable, IntWritable> {
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			//read lines seperated by commas
			Scanner s = new Scanner(value.toString()).useDelimiter(",");
			String line = s.nextLine();
			if (!line.endsWith(":")) {
				System.out.println(line);
				String[] splitString = line.split(",");
				System.out.println(splitString);
				int rating = Integer.parseInt(splitString[1]);	
				int user = Integer.parseInt(splitString[0]);
				//DecimalFormat f = new DecimalFormat("#.##");
				System.out.println(user);
				System.out.println(rating);
				//System.out.println(f.format(rating));
				//emit (user, rating)
				context.write(new IntWritable(user), new IntWritable(rating));
			}
			s.close();
		}
	}
//First Mapper Ends

    //First Reducer Starts
//reduce ( user, ratings ):
//count = 0
//sum = 0
//for n in ratings
//count++
//sum += n
//emit( user, (int)(sum/count*10) )
	public static class Reducer1 extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		@Override
		public void reduce(IntWritable user, Iterable<IntWritable> ratings, Context context)
				throws IOException, InterruptedException {
            int  count = 0;
            int sum = 0;
			for (IntWritable r : ratings) {
                count++;
                sum += r.get();
			}
			context.write(user, new IntWritable((sum*10)/count));	//output is saved in tmp folder
		}
	} 
//First Reducer Ends
	
	//Second Mapper Starts
//map ( user, line ):
//read 2 integers from the line into the variables user and rating (delimiter is tab "\t")
//emit( rating, 1 )
public static class Mapper2 extends Mapper<Object, Text, DoubleWritable, IntWritable> {
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		//read lines seperated by \t
		Scanner s = new Scanner(value.toString()).useDelimiter("\t");
		int user = s.nextInt();
		double rating = s.nextDouble();
		//emit(rating,1)
		context.write(new DoubleWritable(rating),new IntWritable(1));
		s.close();
		}
	}
//Second Mapper Ends

	//Second Reducer Starts
//reduce ( rating, values ):
//sum = 0
//for v in values
//sum += v
//emit( rating/10.0, sum )
public static class Reducer2 extends Reducer<DoubleWritable, IntWritable, DoubleWritable, IntWritable> {
	@Override
	public void reduce(DoubleWritable rating, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable v : values) {
			sum += v.get();
		 }
		//emit(rating/10.0, sum )
		context.write(new DoubleWritable(rating.get() /10.0), new IntWritable(sum));
		}
	}
//Second Reducer Ends

//Main Starts for both the Mappers and Reducers
//Exception Handler
    
    //Job for Mapper1 and Reducer1
    public static void main ( String[] args ) throws Exception {
	    Job job1 = Job.getInstance();
	    job1.setJobName("Job1");
	    job1.setJarByClass(Netflix.class);
	    job1.setMapOutputKeyClass(IntWritable.class);
	    job1.setMapOutputValueClass(IntWritable.class);
	    job1.setOutputKeyClass(IntWritable.class);
	    job1.setOutputValueClass(IntWritable.class);
	    job1.setMapperClass(Mapper1.class);
	    job1.setReducerClass(Reducer1.class);
	    job1.setInputFormatClass(TextInputFormat.class);
	    job1.setOutputFormatClass(TextOutputFormat.class);
	    FileInputFormat.setInputPaths(job1, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job1, new Path(args[1]));
	    job1.waitForCompletion(true);
    //Job for Mapper2 and Reducer2

	    Job job2 = Job.getInstance();
	    job2.setJobName("Job2");
	    job2.setJarByClass(Netflix.class);
	    job2.setMapOutputKeyClass(DoubleWritable.class);
	    job2.setMapOutputValueClass(IntWritable.class);
	    job2.setOutputKeyClass(DoubleWritable.class);
	    job2.setOutputValueClass(IntWritable.class);
	    job2.setMapperClass(Mapper2.class);
	    job2.setReducerClass(Reducer2.class);
	    job2.setInputFormatClass(TextInputFormat.class);
	    job2.setOutputFormatClass(TextOutputFormat.class);
	    FileInputFormat.setInputPaths(job2, new Path(args[1]));
	    FileOutputFormat.setOutputPath(job2, new Path(args[2]));
	    job2.waitForCompletion(true);
    }
}
