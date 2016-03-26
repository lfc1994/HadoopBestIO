package org.apache.hadoop.raid;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Created by JackJay on 16/3/17.
 */
public class HadamardDecoder extends Decoder{
	private HadamardCode hadamardCode;

	//InputFormat
	static class HadamardInputFormat extends SequenceFileInputFormat<LongWritable, Text>{
		@Override
		public List<InputSplit> getSplits(JobContext job) throws IOException {
			return super.getSplits(job);
		}
	}

	//Mapper
	static class HadamardDecodeMapper extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			super.map(key, value, context);
		}
	}

	public HadamardDecoder(Configuration conf) {
		super(conf, Codec.getCodec("hadamard"));
		hadamardCode = new HadamardCode();
		hadamardCode.init(codec);
	}

	@Override
	long fixErasedBlockImpl(FileSystem srcFs, Path srcFile,
							FileSystem parityFs, Path parityFile,
							boolean fixSource, long blockSize,
							long errorOffset, long limit, boolean partial,
							OutputStream out, Progressable reporter, CRC32 crc) throws IOException {

		return 0;
	}
}
