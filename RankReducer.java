package PageRank;



import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class RankReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {

	private static final double damping = 0.85F;
	private double count;

	@Override
	public void configure(JobConf job) {
		count = Integer.parseInt(job.get(PageRank.PAGE_COUNT));
	}

	public void reduce(Text page, Iterator<Text> values,
			OutputCollector<Text, Text> out, Reporter reporter)
			throws IOException {
		if (page.toString().trim().isEmpty())
			return;
		String[] tokens;
		double otherPages = 0;
		StringBuilder links = new StringBuilder("");
		String pageRow;
		while (values.hasNext()) {
			pageRow = values.next().toString();
			if (pageRow.startsWith("|")) {
				links.append("\t").append(pageRow.substring(1));
				continue;
			}

			tokens = pageRow.split("\\t");

			double initPageRank = Double.valueOf(tokens[1]);
			double noOfOutLinks = Double.valueOf(tokens[2]);

			otherPages = otherPages + (initPageRank / noOfOutLinks);
		}

		double latestRank = damping * otherPages + (1.0 - damping)/count;
		out.collect(page, new Text(latestRank + links.toString()));
	}

}
