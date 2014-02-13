package PageRank;



import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class RedLinkMapperAndReducer {

	public static class RedLinkMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable arg0, Text value,
				OutputCollector<Text, Text> out, Reporter arg3)
				throws IOException {
			String[] tokens = value.toString().split("\t");
			int i = 1;
			while (i < tokens.length) {
				out.collect(new Text(tokens[i++]), new Text(tokens[0]));
			}
		}
	}

	public static class RedLinkReducer extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text arg0, Iterator<Text> iterText,
				OutputCollector<Text, Text> out, Reporter arg3)
				throws IOException {
			StringBuilder values = new StringBuilder(" ");
			boolean check = true;
			String link;
			while (iterText.hasNext()) {
				link = iterText.next().toString();
				if (link.startsWith("redLinks"))
					continue;
				if (!check)
					values.append("\t");
				values.append(link);
				check = false;
			}
			out.collect(arg0, new Text(values.toString()));
		}
	}
}
