package PageRank;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class PageRank {

	private static NumberFormat nf = new DecimalFormat("0");
	public static final String PAGE_COUNT = "page_count";
	private Configuration conf = new Configuration();
	public static void main(String[] args) throws Exception {
		PageRank pr = new PageRank();
		String bucketName = args[0];
		String countPath = bucketName + "tmp/count/part-00000";
		pr.ParseXML("s3://spring-2014-ds/data", bucketName + "tmp/red");
		pr.removeRedLinks(bucketName + "tmp/red", bucketName + "tmp/iter0");
		pr.countPages(bucketName + "tmp/iter0", bucketName + "tmp/count");
		int runs = 0;
		
		for (; runs < 8; runs++) {
			pr.runRankCalculation(bucketName + "tmp/iter" + nf.format(runs),
					bucketName + "tmp/iter" + nf.format(runs + 1), countPath);
			if (runs == 0 || runs == 7 )
				pr.orderRank(bucketName + "tmp/iter" + nf.format(runs + 1), 
						bucketName + "tmp/result" + nf.format(runs + 1),
								countPath);
		}
		pr.move(bucketName);

	}
	
	private void removeRedLinks(String inputPath, String outputPath) 
				throws IOException {
		JobConf conf = new JobConf(PageRank.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setMapperClass(RedLinkMapperAndReducer.RedLinkMapper.class);
		conf.setReducerClass(RedLinkMapperAndReducer.RedLinkReducer.class);

		JobClient.runJob(conf);
	}

	private void move(String bucketName) throws Exception {
		FileSystem fs = null;
		// for outlinks
		Path src;
		Path dst;
		try {
			src = new Path(bucketName + "tmp/iter0");
			dst = new Path(bucketName + "results/PageRank.outlink.out");
			fs = src.getFileSystem(conf);
			FileUtil.copyMerge(fs, src, fs, dst, false, conf, null);
		} catch (IOException e) {
			e.printStackTrace();
		}
		// for first iteration
		src = new Path(bucketName + "tmp/result1/part-00000");
		dst = new Path(bucketName + "results/PageRank.iter1.out");
		fs.rename(src, dst);
		// for last iteration
		src = new Path(bucketName + "tmp/result8/part-00000");
		dst = new Path(bucketName + "results/PageRank.iter8.out");
		fs.rename(src, dst);
		// for count 
		src = new Path(bucketName + "tmp/count/part-00000");
		dst = new Path(bucketName + "results/PageRank.n.out");
		fs.rename(src, dst);
		fs.close();
	}

	private Integer readCountFromFile(String fileName) {
		Integer count = 0;
		Configuration conf = new Configuration();
		BufferedReader br = null;
		FileSystem fs = null;
		Path path = new Path(fileName);
		try {
			fs = path.getFileSystem(conf);
			br = new BufferedReader(new InputStreamReader(fs.open(path)));
			String line = br.readLine();
			if (line != null && !line.isEmpty()) {
				count = Integer.parseInt(line.substring(line.lastIndexOf('=') + 2,
										 line.indexOf('\t')));
				return count;
			}
		} catch (IOException e) {
			//e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
				if (fs != null)
					fs.close();
			} catch (IOException e) {
				// e.printStackTrace();
			}
		}
		return count;

	}

	public void ParseXML(String inputPath, String outputPath)
			throws IOException {
		JobConf conf = new JobConf(PageRank.class);

		conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
		conf.set(XmlInputFormat.END_TAG_KEY, "</page>");

		// Set Input and output parameters
		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(XmlInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(WikiMapper.class);
		conf.setReducerClass(WikiReducer.class);

		JobClient.runJob(conf);
	}

	private void countPages(String inputPath, String outputPath)
			throws IOException {
		JobConf conf = new JobConf(PageRank.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setNumReduceTasks(1);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setMapperClass(CountMapperReducer.CountMapper.class);
		conf.setReducerClass(CountMapperReducer.CountReducer.class);

		JobClient.runJob(conf);
	}

	private void runRankCalculation(String inputPath, String outputPath,
			String countPath) throws IOException {
		JobConf conf = new JobConf(PageRank.class);

		Integer count = readCountFromFile(countPath);
		conf.set(PAGE_COUNT, count.toString());
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setMapperClass(RankMapper.class);
		conf.setReducerClass(RankReducer.class);

		JobClient.runJob(conf);
	}

	private void orderRank(String inputPath, String outputPath, String countPath)
			throws IOException {
		JobConf conf = new JobConf(PageRank.class);
		Integer count = readCountFromFile(countPath);
		conf.set(PAGE_COUNT, count.toString());
		conf.setNumReduceTasks(1);
		conf.setOutputKeyClass(DoubleWritable.class);
		conf.setOutputValueClass(Text.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setMapperClass(PageRankMapper.class);
		conf.setReducerClass(PageRankReducer.class);

		JobClient.runJob(conf);
	}

}
