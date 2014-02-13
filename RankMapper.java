package PageRank;



import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class RankMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, Text> {
	
	private double count;
	private double startRank;
	@Override
    public void configure(JobConf job)
    {
    	count = Integer.parseInt(job.get(PageRank.PAGE_COUNT));
    	startRank = (1.0)/count;
    }
	
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> out, Reporter reporter)
			throws IOException {
		int pageTIndex = value.find("\t");
		int rankTIndex = value.find(" ", pageTIndex+1);
		
		String page = Text.decode(value.getBytes(), 0, pageTIndex);
		String pageWithRank = null;
		if (rankTIndex != -1)
		{
			pageWithRank = page + '\t' + startRank + '\t';
		}
		else
		{
			rankTIndex = value.find("\t", pageTIndex + 1);
			pageWithRank = Text.decode(value.getBytes(), 0, rankTIndex + 1);
		}
		if (rankTIndex == -1)
			return;
		String normalLinks;
		normalLinks = Text.decode(value.getBytes(), rankTIndex + 1,
								  value.getLength() - (rankTIndex + 1));
		String[] otherPages = normalLinks.split("\\t");
		int totalLinks = otherPages.length;

		for (String otherPage : otherPages) {
			Text pageRankWithTotalLinks = new Text(pageWithRank + totalLinks);
			out.collect(new Text(otherPage), pageRankWithTotalLinks);
		}
		out.collect(new Text(page), new Text("|" + normalLinks));
	}
}
