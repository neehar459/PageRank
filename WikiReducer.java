package PageRank;



import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class WikiReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> out, Reporter reporter)
			throws IOException {
		boolean firstOccurence = true;
		boolean redLink = true;
		String val;
		StringBuilder total = new StringBuilder("");
		while (values.hasNext()) {
			 val = values.next().toString();
			if (val.equals("#"))
			{	
				redLink = false;
				continue;
			}
			else
			{
				if (!firstOccurence)
					total.append("\t");
				total.append(val);
				firstOccurence = false;
			}
		}
		if (!redLink)
			out.collect(key, new Text(total.toString()));
		else
			out.collect(new Text("redLinks"  + key.toString()), new Text(total.toString()));
	}
}
