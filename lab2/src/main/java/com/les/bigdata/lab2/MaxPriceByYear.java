package com.les.bigdata.lab2;

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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class MaxPriceByYear {

    private static int DATE_INDEX = 0;
    private static int MAX_PRICE_INDEX = 3;
    private static int FIELDS_COUNT = 7;

    private static final Text MARKET_KEY1 = new Text("EURUSD");
    private static final Text MARKET_KEY2 = new Text("EURGBP");

    public static class MaxByYear implements Writable {

        private int year;
        private double maxPrice;

        public MaxByYear() {
        }

        public MaxByYear(int year, double maxPrice) {
            this.year = year;
            this.maxPrice = maxPrice;
        }

        public int getYear() {
            return year;
        }

        public double getMaxPrice() {
            return maxPrice;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(year);
            out.writeDouble(maxPrice);

        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.year = in.readInt();
            this.maxPrice = in.readInt();
        }

        @Override
        public String toString() {
            return "{year=" + year + ", maxPrice=" + maxPrice + "}";
        }
    }

    public static class MaxByYearMapper extends Mapper<Object, Text, Text, MaxByYear> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String csvLine = value.toString();
            String[] csvField = csvLine.split(",");
            context.write(MARKET_KEY1, new MaxByYear(getYear(csvField, 0), getMaxPrice(csvField, 0)));
            context.write(MARKET_KEY2, new MaxByYear(getYear(csvField, 1), getMaxPrice(csvField, 1)));
        }

        private int getYear(String[] csv, int pair) {
            return Integer.parseInt(csv[DATE_INDEX + pair * FIELDS_COUNT].split("\\.")[0]);
        }

        private double getMaxPrice(String[] cvs, int pair) {
            return Double.parseDouble(cvs[MAX_PRICE_INDEX + pair * FIELDS_COUNT]);
        }
    }

    public static class MaxByYearReducer extends Reducer<Text, MaxByYear, Text, MaxByYear[]> {

        public void reduce(Text key, Iterable<MaxByYear> values, Context context) throws IOException, InterruptedException {

            MaxByYear[] listMaxByYear = StreamSupport.stream(values.spliterator(), false)
                    .collect(
                            Collectors.toMap(
                                    MaxByYear::getYear,
                                    MaxByYear::getMaxPrice,
                                    Math::max
                            )
                    )
                    .entrySet().stream()
                    .map(entry -> new MaxByYear(entry.getKey(), entry.getValue()))
                    .toArray(MaxByYear[]::new);

            context.write(key, listMaxByYear);
        }

    }

    public static void main(String... args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: MaxByYear <hdfs://> <in> <out>");
            System.exit(2);
        }

        FileSystem hdfs = FileSystem.get(new URI(args[0]), conf);
        Path resultFolder = new Path(args[2]);
        if (hdfs.exists(resultFolder))
            hdfs.delete(resultFolder, true);

        Job job = Job.getInstance(conf, "Market Max Price by Year");
        job.setJarByClass(MaxPriceByYear.class);
        job.setMapperClass(MaxByYearMapper.class);
        job.setCombinerClass(MaxByYearReducer.class);
        job.setReducerClass(MaxByYearReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MaxByYear[].class);

        for (int i = 1; i < otherArgs.length - 1; i++) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[(otherArgs.length - 1)]));

        boolean finishState = job.waitForCompletion(true);
        System.out.println("Job Running Time: " + (job.getFinishTime() - job.getStartTime()));

        System.exit(finishState ? 0 : 1);
    }

}
