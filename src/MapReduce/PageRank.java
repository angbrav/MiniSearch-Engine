package cc.ist;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PageRank {

	public static double d = 0.85;
	public static double threshold = 0.001;
	public static int maxiter = 100;

	public static class Map extends
			Mapper<LongWritable, Text, LongWritable, Text> {
		private final static LongWritable node = new LongWritable(0);
		private Text partial = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			int degree;
			double currentPR;
			String outputValue, nodeid, partialPR;
			String line = value.toString();
			String link = "";
			String token;

			StringTokenizer tokenizer = new StringTokenizer(line);
			nodeid = tokenizer.nextToken();
			currentPR = Double.parseDouble(tokenizer.nextToken());
			tokenizer.nextToken(); // Previous Page Rank
			degree = Integer.parseInt(tokenizer.nextToken());
			partialPR = Double.toString(currentPR / degree);

			partial.set("!" + " " + Integer.toString(degree) + " "
					+ Double.toString(currentPR));
			node.set(Long.parseLong(nodeid));
			context.write(node, partial);

			while (tokenizer.hasMoreTokens()) {

				token = tokenizer.nextToken();
				link = link + " " + token;
				node.set(Long.parseLong(token));
				outputValue = nodeid + " " + partialPR + " "
						+ Integer.toString(degree);
				partial.set(outputValue);
				context.write(node, partial);
			}
			partial.set("|" + link);
			node.set(Long.parseLong(nodeid));
			context.write(node, partial);
		}
	}

	public static class Reduce extends
			Reducer<LongWritable, Text, LongWritable, Text> {

		private Text output = new Text();

		public void reduce(LongWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			double sum = 0.0;
			boolean exist = false;
			String line, links, degree, previousPR = "";
			StringTokenizer tokenizer;
			degree = "";
			links = "";

			for (Text val : values) {
				line = val.toString();

				if (line.startsWith("!")) {
					exist = true;
					tokenizer = new StringTokenizer(line);
					tokenizer.nextToken();
					degree = tokenizer.nextToken();
					previousPR = tokenizer.nextToken();
				} else {
					if (line.startsWith("|")) {
						links = line.substring(1);
					} else {
						tokenizer = new StringTokenizer(line);
						tokenizer.nextToken();
						sum += Double.parseDouble(tokenizer.nextToken());
					}
				}

			}
			if (exist) {
				sum = (1 - PageRank.d) + PageRank.d * (sum);
				output.set(Double.toString(sum) + " " + previousPR + " "
						+ degree + links);
				context.write(key, output);
			}
		}
	}

	public static class MapCheck extends
			Mapper<LongWritable, Text, IntWritable, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private IntWritable partial = new IntWritable(0);

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			double currentPR, previousPR;
			String line = value.toString();

			StringTokenizer tokenizer = new StringTokenizer(line);
			tokenizer.nextToken();
			currentPR = Double.parseDouble(tokenizer.nextToken());
			previousPR = Double.parseDouble(tokenizer.nextToken());

			if (Math.abs(previousPR - currentPR) < PageRank.threshold) {
				partial.set(1);
			}
			context.write(one, partial);

		}
	}

	public static class ReduceCheck extends
			Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

		private IntWritable output = new IntWritable(0);

		public void reduce(IntWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			int counter = 0;
			;

			for (IntWritable val : values) {
				if (val.get() == 0) {
					counter++;
				}
			}
			if (counter == 0) {
				output.set(1);
			}
			context.write(key, output);

		}
	}

	public static void main(String[] args) throws Exception {

		int iterator = 0;
		if (args.length == 2) {
			PageRank.maxiter = Integer.parseInt(args[1]);
		}
		do {
			Job pageRankJob = PageRank.getPageRankJob(args[0] + "/iter"
					+ Integer.toString(iterator), args[0] + "/iter"
					+ Integer.toString(iterator + 1));
			pageRankJob.waitForCompletion(true);
			iterator++;
			Job checkJob = PageRank.getCheckIterationJob(args[0] + "/iter"
					+ Integer.toString(iterator), args[0] + "/check/iter"
					+ Integer.toString(iterator));
			checkJob.waitForCompletion(true);
		} while (!PageRank.canStopIters(args[0] + "/check/iter"
				+ Integer.toString(iterator) + "/", iterator));
		PageRank.deleteFolder(args[0] + "/check");
		PageRank.changeOutputFolder(iterator, args[0]);
	}

	// get a pagerank job
	public static Job getPageRankJob(String inputPath, String outputPath)
			throws Exception {

		Configuration conf = new Configuration();

		Job job = new Job(conf, "pagerank");
		job.setJarByClass(PageRank.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		return job;
	}

	public static Job getCheckIterationJob(String inputPath, String outputPath)
			throws Exception {

		Configuration conf = new Configuration();

		Job job = new Job(conf, "checkiteration");
		job.setJarByClass(PageRank.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(MapCheck.class);
		job.setReducerClass(ReduceCheck.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		return job;
	}

	public static void deleteFolder(String path) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(path), conf);
		fs.delete(new Path(path), true);
	}

	public static void changeOutputFolder(int iterator, String path)
			throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(path), conf);
		fs.rename(new Path(path + "/iter" + Integer.toString(iterator)),
				new Path(path + "/output"));
	}

	public static boolean canStopIters(String checkPath, int currentiter)
			throws IOException {
		if (currentiter >= PageRank.maxiter) {
			return true;
		}else{
			return false;
		}

//		Configuration conf = new Configuration();
//		FileSystem fs = FileSystem.get(URI.create(checkPath), conf);
//
//		FileStatus files[] = fs.listStatus(new Path(checkPath));
//		FileStatus max = files[0];
//		for (int i = 0; i < files.length; i++) {
//			if (files[i].getBlockSize() > max.getBlockSize()) {
//				max = files[0];
//			}
//		}
//		FSDataInputStream fsi = fs.open(max.getPath());
//		BufferedReader reader = new BufferedReader(new InputStreamReader(fsi));
//		String temp = reader.readLine();
//		if (!temp.isEmpty()) {
//			String[] results = temp.split("\t");
//			if (Double.valueOf(results[1]) == 1) {
//				return true;
//			} else {
//				return false;
//			}
//		}
//		return false;
	}

}