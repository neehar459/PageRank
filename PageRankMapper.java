package PageRank;



import java.io.IOException;
import java.nio.charset.CharacterCodingException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class PageRankMapper extends MapReduceBase implements 
				Mapper<LongWritable, Text, DoubleWritable, Text> {
    
	private double count;
	@Override
    public void configure(JobConf job)
    {
    	count = Integer.parseInt(job.get(PageRank.PAGE_COUNT));
    }
	
    public void map(LongWritable key, Text value, 
    			OutputCollector<DoubleWritable, Text> output, Reporter arg3) {
        String[] rows = getRowValue(key, value);
        
        Double parseNumberF = Double.parseDouble(rows[1]);
        
        Text page = new Text(rows[0]);
        DoubleWritable rank = new DoubleWritable(parseNumberF);
        
        try {
        	if (rank.get() >= (5.0/count))
        		output.collect(new DoubleWritable(rank.get() * -1.0), page);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
		}
    }
    
    private String[] getRowValue(LongWritable key, Text value) {
        String[] row = new String[2];
        int tabPIndex = value.find("\t");
        int tabRIndex = value.find("\t", tabPIndex + 1);
        
        // no tab after rank (when there are no links)
        int last;
        if (tabRIndex == -1) {
            last = value.getLength() - (tabPIndex + 1);
        } else {
            last = tabRIndex - (tabPIndex + 1);
        }
        
        try {
			row[0] = Text.decode(value.getBytes(), 0, tabPIndex);
			row[1] = Text.decode(value.getBytes(), tabPIndex + 1, last);
		} catch (CharacterCodingException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
		}
        
        return row;
    }
    
}
