package PageRank;



import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

public class XmlInputFormat extends TextInputFormat {

	public static final String START_TAG_KEY = "xmlinput.start";
	public static final String END_TAG_KEY = "xmlinput.end";

	@Override
	public RecordReader<LongWritable, Text> getRecordReader(
			InputSplit inputSplit, JobConf jobConf, Reporter reporter)
			throws IOException {
		return new XmlRecordReader((FileSplit) inputSplit, jobConf);
	}
	public static class XmlRecordReader implements
			RecordReader<LongWritable, Text> {
		private final byte[] startTag;
		private final byte[] endTag;
		private final long start;
		private final long end;
		private final FSDataInputStream fileSInputStream;
		private final DataOutputBuffer buffer = new DataOutputBuffer();

		public XmlRecordReader(FileSplit split, JobConf jobConf)
				throws IOException {
			startTag = jobConf.get(START_TAG_KEY).getBytes("utf-8");
			endTag = jobConf.get(END_TAG_KEY).getBytes("utf-8");
			start = split.getStart();
			end = start + split.getLength();
			Path file = split.getPath();
			FileSystem fs = file.getFileSystem(jobConf);
			fileSInputStream = fs.open(split.getPath());
			fileSInputStream.seek(start);
		}

		public boolean next(LongWritable key, Text value) throws IOException {
			if (fileSInputStream.getPos() < end) {
				if (readUntilMatch(startTag, false)) {
					try {
						buffer.write(startTag);
						if (readUntilMatch(endTag, true)) {
							key.set(fileSInputStream.getPos());
							value.set(buffer.getData(), 0, buffer.getLength());
							return true;
						}
					} finally {
						buffer.reset();
					}
				}
			}
			return false;
		}

		public LongWritable createKey() {
			return new LongWritable();
		}

		public Text createValue() {
			return new Text();
		}

		public long getPos() throws IOException {
			return fileSInputStream.getPos();
		}

		public void close() throws IOException {
			fileSInputStream.close();
		}

		public float getProgress() throws IOException {
			return (fileSInputStream.getPos() - start) / (float) (end - start);
		}

		private boolean readUntilMatch(byte[] match, boolean withinBlock)
				throws IOException {
			int i = 0;
			while (true) {
				int b = fileSInputStream.read();
				if (b == -1)
					return false;
				if (withinBlock)
					buffer.write(b);
				if (b == match[i]) {
					i++;
					if (i >= match.length)
						return true;
				} else
					i = 0;
				if (!withinBlock && i == 0 && fileSInputStream.getPos() >= end)
					return false;
			}
		}
	}
}
