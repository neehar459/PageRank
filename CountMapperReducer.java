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

public class CountMapperReducer {

	public static class CountMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		private static final Text COUNTER = new Text("COUNT");
		private static final Text ONE = new Text("1");

		@Override
		public void map(LongWritable arg0, Text text,
				OutputCollector<Text, Text> out, Reporter arg3)
				throws IOException {
			out.collect(COUNTER, ONE);

		}
	}

	public static class CountReducer extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text arg0, Iterator<Text> iterText,
				OutputCollector<Text, Text> out, Reporter arg3)
				throws IOException {
			Integer count = 0;
			while (iterText.hasNext()) {
				iterText.next();
				count++;
			}
			out.collect(new Text("N = " + count.toString()), new Text(""));
		}
	}
}
