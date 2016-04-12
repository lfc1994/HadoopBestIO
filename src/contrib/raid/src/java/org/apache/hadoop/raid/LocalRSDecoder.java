package org.apache.hadoop.raid;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.zip.CRC32;

/**
 * Created by JackJay on 16/4/8.
 */
public class LocalRSDecoder extends Decoder{

	private int[] erasedLocationsArray = null;
	private int[] locationsToReadArray = null;
	private int[] locationsNotToReadArray = null;
	private List<Integer> erasedLocations = null;
	private List<Integer> locationsToRead = null;
	private static final String rsStr =
			"      {   \n" +
			"        \"id\"            : \"rs\",\n" +
			"        \"parity_dir\"    : \"/raidrs\",\n" +
			"        \"stripe_length\" : 3,\n" +
			"        \"parity_length\" : 1,\n" +
			"        \"priority\"      : 300,\n" +
			"        \"erasure_code\"  : \"org.apache.hadoop.raid.ReedSolomonCode\",\n" +
			"        \"description\"   : \"ReedSolomonCode code\",\n" +
			"        \"simulate_block_fix\"  : true,\n" +
			"      }";
	private Codec rsCodec = null;
	public LocalRSDecoder(Configuration conf) {
		super(conf, Codec.getCodec("rs_local"));
		try {
			rsCodec = new Codec(new JSONObject(rsStr));
		} catch (JSONException e) {
			LOG.info("初始化3,1 rsCodec失败");
		}
	}

	@Override
	long fixErasedBlockImpl(FileSystem srcFs, Path srcFile, FileSystem parityFs, Path parityFile,
							boolean fixSource, long blockSize, long errorOffset,
							long limit, boolean partial, OutputStream out,
							Progressable reporter, CRC32 crc) throws IOException {
		long startTime = System.currentTimeMillis();//纪录开始时间
		if (crc != null) crc.reset();// 重置crc

		/**
		 * 初始化erasedLocations
		 */
		int blockIdx = (int) (errorOffset / blockSize);// 第几块
		StripeReader.LocationPair lp = null;//记录stripeIdx和blockIdx的对应关系
		int erasedLocationToFix;
		//一个条带中,校验block在数据block之前.计算起始错误块在一个条带中的位置索引
		if (fixSource) {
			// 计算当前坏块所在stripe,以locationPair数据结构记录
			lp = StripeReader.getBlockLocation(rsCodec, srcFs, srcFile, blockIdx, conf);
			// 当前坏块在 strip中的索引(parity块在前)
			erasedLocationToFix = rsCodec.parityLength + lp.getBlockIdxInStripe();
		} else {
			lp = StripeReader.getParityBlockLocation(rsCodec, blockIdx);
			erasedLocationToFix = lp.getBlockIdxInStripe();
		}
		erasedLocations = new ArrayList<Integer>();
		locationsToRead = new ArrayList<Integer>(rsCodec.parityLength + rsCodec.stripeLength);
		erasedLocations.add(erasedLocationToFix);
		LOG.info("Need to write " + limit +
				" bytes for erased location index " + erasedLocationToFix);


		/**
		 * 初始化其他变量
		 */
		FileStatus srcStat = srcFs.getFileStatus(srcFile);
		FileStatus parityStat = parityFs.getFileStatus(parityFile);
		InputStream[] inputs = null;
		ParallelStreamReader parallelReader = null;
		long startOffsetInBlock = 0;
		if (partial) startOffsetInBlock = errorOffset % blockSize;
		int numReadBytes = 0;
		long written;

		try {
			for (written = 0; written < limit; ) {
				try {

					/**
					 * 创建并启动并行流读取器
					 */
					if (parallelReader == null) {
						long offsetInBlock = written + startOffsetInBlock;
						StripeReader sReader = StripeReader.getStripeReader(rsCodec,
								conf, blockSize, srcFs, lp.getStripeIdx(), srcStat);
						// 创建输入流,更新erasedLocations
						inputs = sReader.buildInputs(srcFs, srcFile,
								srcStat, parityFs, parityFile, parityStat,
								lp.getStripeIdx(), offsetInBlock, erasedLocations,
								locationsToRead, code);
						LOG.info("Erased locations: " + erasedLocations.toString() +
								"\nLocations to Read for repair:" +
								locationsToRead.toString());

						// 启动parallReader
						this.writeBufs = new byte[erasedLocations.size()][];
						for (int i = 0; i < writeBufs.length; i++) {
							writeBufs[i] = new byte[bufSize];
						}
						parallelReader = new ParallelStreamReader(reporter, inputs,
								(int) Math.min(bufSize, limit),
								parallelism, 2, Math.min(limit, blockSize));
						parallelReader.start();
					}

					/**
					 * 读取帮助数据并解码,最终写入临时块文件
					 */
					// read过程中捕获到的异常将会抛出,并添加错误位置到erasedLocations
					ParallelStreamReader.ReadResult readResult = readFromInputs(
							erasedLocations, limit, reporter, parallelReader);
					fillLocations();
					code.decodeBulk(readResult.readBufs, writeBufs, erasedLocationsArray,
							locationsToReadArray, locationsNotToReadArray);

					// 更新numReadBytes
					for (int readNum : readResult.numRead) numReadBytes += readNum;

					// 使用输出流out写解码数据,并更新crc
					int toWrite = (int) Math.min((long) bufSize, limit - written);
					for (int i = 0; i < erasedLocationsArray.length; i++) {
						if (erasedLocationsArray[i] == erasedLocationToFix) {
							if (out != null)
								out.write(writeBufs[i], 0, toWrite);
							if (crc != null) {
								crc.update(writeBufs[i], 0, toWrite);
							}
							written += toWrite;
							break;
						}
					}

				} catch (IOException e) {
					if (e instanceof TooManyErasedLocations) {
						logRaidReconstructionMetrics("FAILURE", 0, rsCodec,
								System.currentTimeMillis() - startTime,
								erasedLocations.size(), numReadBytes, srcFile, errorOffset,
								RaidNode.LOGTYPES.OFFLINE_RECONSTRUCTION, srcFs);
						throw e;
					}
					// Re-create inputs from the new erased locations.
					if (parallelReader != null) {
						parallelReader.shutdown();
						parallelReader = null;
					}
					RaidUtils.closeStreams(inputs);
				}
			}
			//更新raid重构监控数据
			logRaidReconstructionMetrics("SUCCESS", written, rsCodec,
					System.currentTimeMillis() - startTime,
					erasedLocations.size(), numReadBytes, srcFile, errorOffset,
					RaidNode.LOGTYPES.OFFLINE_RECONSTRUCTION, srcFs);
			return written;
		} finally {
			if (parallelReader != null) parallelReader.shutdown();
			RaidUtils.closeStreams(inputs);
		}
	}

	private void fillLocations(){
		int i = 0;
		erasedLocationsArray = new int[erasedLocations.size()];
		for (int loc = 0; loc < rsCodec.stripeLength + rsCodec.parityLength;
			 loc++) {
			if (erasedLocations.indexOf(loc) >= 0) {
				erasedLocationsArray[i] = loc;
				i++;
			}
		}
		i = 0;
		locationsToReadArray = new int[locationsToRead.size()];
		for (int loc = 0; loc < rsCodec.stripeLength + rsCodec.parityLength;
			 loc++) {
			if (locationsToRead.indexOf(loc) >= 0) {
				locationsToReadArray[i] = loc;
				i++;
			}
		}
		i = 0;
		locationsNotToReadArray = new int[rsCodec.stripeLength +
				rsCodec.parityLength -
				locationsToRead.size()];
		for (int loc = 0; loc < rsCodec.stripeLength + rsCodec.parityLength;
			 loc++) {
			if (locationsToRead.indexOf(loc) == -1 ||
					erasedLocations.indexOf(loc) != -1) {
				locationsNotToReadArray[i] = loc;
				i++;
			}
		}

	}
}
