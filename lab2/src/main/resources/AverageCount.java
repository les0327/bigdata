import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class AverageCount {
	
	private static final Text MARKET_KEY1=new Text("EURUSD");
	private static final Text MARKET_KEY2=new Text("EURGBP");
	private static final Text MARKET_KEY3=new Text("EURCHF");
	
	public static class MarketAverage implements Writable {

		private double average;
		private int count;
		
		public MarketAverage() {
			
		}
		
		public MarketAverage(double average, int count) {
			this.average=average;
			this.count=count;
		}
		
		public double getAverage() {
			return average;
		}

		public int getCount() {
			return count;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeDouble(average);
			out.writeInt(count);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			average=in.readDouble();
			count=in.readInt();
		}
		
		@Override
		public String toString() {
			return new String(average+" ("+count+")");
		}
		
	}
	
	public static class AverageCountMapper extends Mapper<Object, Text, Text, MarketAverage>{
				 
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String csvLine = value.toString();
			String[] csvField = csvLine.split(",");
			context.write(MARKET_KEY1, new MarketAverage(Double.parseDouble(csvField[6]), 1));
			context.write(MARKET_KEY2, new MarketAverage(Double.parseDouble(csvField[13]), 1));
			context.write(MARKET_KEY3, new MarketAverage(Double.parseDouble(csvField[20]), 1));
		}
	 }

	 public static class AverageCountReducer extends Reducer<Text, MarketAverage, Text, MarketAverage>{
		 
		public void reduce(Text key, Iterable<MarketAverage> values, Context context) throws IOException, InterruptedException {
			 
			double sum=0;
			int count=0;
			 
			for(MarketAverage marketAverage: values) {
				sum+=(marketAverage.getAverage()*marketAverage.getCount());
				count+=marketAverage.getCount();
			} 
			 
			context.write(key, new MarketAverage(sum/count, count));
		}
		 
	 }

	 public static void main(String... args) throws Exception{
		 
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: AverageCount <hdfs://> <in> <out>");
			System.exit(2);
		}
		
		FileSystem hdfs=FileSystem.get(new URI(args[0]), conf);
		Path resultFolder=new Path(args[2]);
		if(hdfs.exists(resultFolder))
			hdfs.delete(resultFolder, true);

		Job job = Job.getInstance(conf, "Market Average Count");
		job.setJarByClass(AverageCount.class);
		job.setMapperClass(AverageCountMapper.class);
		job.setCombinerClass(AverageCountReducer.class);
		job.setReducerClass(AverageCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MarketAverage.class);
		
		for (int i = 1; i < otherArgs.length - 1; i++) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[(otherArgs.length - 1)]));

		boolean finishState = job.waitForCompletion(true);
		System.out.println("Job Running Time: " + (job.getFinishTime() - job.getStartTime()));

		System.exit(finishState ? 0 : 1);
	 }
	 
}
