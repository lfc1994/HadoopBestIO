package org.apache.hadoop.raid;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by JackJay on 16/3/16.
 * adamard算法编码器
 */
public class HadamardEncoder extends Encoder {

	private HadamardCode hadamardCode;
	//初始化编码器
	public HadamardEncoder(Configuration conf, Codec codec) {
		super(conf, codec);
		hadamardCode = new HadamardCode();
		hadamardCode.init(codec);
	}

	//重写encodeStripe函数
	@Override
	void encodeStripe(InputStream[] blocks, long blockSize, OutputStream[] outs,
					  Progressable reporter) throws IOException {

		//configureBuffers(blockSize);
		int boundedBufferCapacity = 1;
		ParallelStreamReader parallelReader = new ParallelStreamReader(
				reporter, blocks, bufSize,
				parallelism, boundedBufferCapacity, blockSize);
		parallelReader.start();
		try {
			for (long encoded = 0; encoded < blockSize; encoded += bufSize) {
				ParallelStreamReader.ReadResult readResult = null;
				try {
					readResult = parallelReader.getReadResult();
				} catch (InterruptedException e) {
					throw new IOException("Interrupted while waiting for read result");
				}
				// Cannot tolerate any IO errors.
				IOException readEx = readResult.getException();
				if (readEx != null) {
					throw readEx;
				}
				hadamardCode.setCurrent_dim(encoded*32/blockSize);
				hadamardCode.encodeBulk(readResult.readBufs, writeBufs);
				reporter.progress();

				// Now that we have some data to write, send it to the temp files.
				for (int i = 0; i < codec.parityLength; i++) {
					outs[i].write(writeBufs[i], 0, bufSize);
					reporter.progress();
				}
			}
		} finally {
			parallelReader.shutdown();
		}
	}
}
