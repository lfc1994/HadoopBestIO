package org.apache.hadoop.raid;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Created by JackJay on 16/4/24.
 */
public class LRC1Decoder extends Decoder {
	public static final Log LOG = LogFactory.getLog("LRC1Decoder");
	private Codec xorCodec = null;
	private LRC1Code lrc1Code= null;
	private int[] erasedLocationsArray = null;
	private int[] locationsToReadArray = null;
	private int[] locationsNotToReadArray = null;
	private List<Integer> erasedLocations = null;
	private List<Integer> locationsToRead = null;

	private class LRC1StripeReader extends FileStripeReader{
		public LRC1StripeReader(Configuration conf, long blockSize, Codec codec,
								FileSystem fs, long stripeStartIdx,
								Path srcFile, long srcSize) {
			super(conf, blockSize, codec, fs, stripeStartIdx, srcFile, srcSize);
			this.stripeStartOffset = stripeStartIdx * 3 * blockSize;
		}

		@Override
		public InputStream[] buildInputs(FileSystem srcFs, Path srcFile, FileStatus srcStat,
										 FileSystem parityFs, Path parityFile, FileStatus parityStat,
										 int stripeIdx, long offsetInBlock, List<Integer> erasedLocations,
										 List<Integer> locationsToRead, ErasureCode code) throws IOException {
			InputStream[] inputs = new InputStream[3 + 2];
			boolean redo = false;
			do {
				locationsToRead.clear();
				locationsToRead.addAll(code.locationsToReadForDecode(erasedLocations));
				for (int i = 0; i < inputs.length; i++) {
					boolean isErased = (erasedLocations.indexOf(i) != -1);
					boolean shouldRead = (locationsToRead.indexOf(i) != -1);
					try {
						InputStream stm = null;
						if (isErased || !shouldRead) {
							if (isErased) {
								LOG.info("Location " + i + " is erased, using zeros");
							} else {
								LOG.info("Location " + i + " need not be read, using zeros");
							}

							stm = new RaidUtils.ZeroInputStream(srcStat.getBlockSize() * (
									(i < 2) ? stripeIdx * 2 + i : stripeIdx * 3 + i - 2));
						} else {
							stm = buildOneInput(i, offsetInBlock,
									srcFs, srcFile, srcStat,
									parityFs, parityFile, parityStat);
						}
						inputs[i] = stm;
					} catch (IOException e) {
						if (e instanceof BlockMissingException || e instanceof ChecksumException) {
							erasedLocations.add(i);
							redo = true;
							RaidUtils.closeStreams(inputs);
							break;
						} else {
							throw e;
						}
					}
				}
			} while (redo);
			return inputs;
		}

		@Override
		protected InputStream getParityFileInput(int locationIndex, Path parityFile, FileSystem parityFs,
												 FileStatus parityStat, long offsetInBlock) throws IOException {
			// Dealing with a parity file here.
			int parityBlockIdx = (int) (2 * stripeStartIdx + locationIndex);
			long offset = parityStat.getBlockSize() * parityBlockIdx + offsetInBlock;
			assert (offset < parityStat.getLen());
			LOG.info("Opening parityFile from offset" + ":" + offset/1024/1024 +
					" for location " + locationIndex);
			FSDataInputStream s = parityFs.open(
					parityFile, conf.getInt("io.file.buffer.size", 64 * 1024));
			s.seek(offset);
			return s;
		}

		@Override
		public InputStream buildOneInput(int locationIndex, long offsetInBlock,
										 FileSystem srcFs, Path srcFile,
										 FileStatus srcStat, FileSystem parityFs,
										 Path parityFile, FileStatus parityStat) throws IOException {
			final long blockSize = srcStat.getBlockSize();

			LOG.info("buildOneInput " + " srclen " + srcStat.getLen() / 1024 / 1024 +
					" paritylen " + parityStat.getLen() / 1024 / 1024 +
					" stripeindex " + stripeStartIdx + " locationindex " + locationIndex +
					" offsetinblock " + offsetInBlock / 1024 / 1024);
			if (locationIndex < 2) {
				return this.getParityFileInput(locationIndex, parityFile,
						parityFs, parityStat, offsetInBlock);
			} else {
				// Dealing with a src file here.
				int blockIdxInStripe = locationIndex - 2;
				int blockIdx = (int)(3 * stripeStartIdx + blockIdxInStripe);
				long offset = blockSize * blockIdx + offsetInBlock;
				if (offset >= srcStat.getLen()) {
					LOG.info("Using zeros for srcFile" + ":" + offset +
							" for location " + locationIndex);
					return new RaidUtils.ZeroInputStream(blockSize * (blockIdx + 1));
				} else {
					LOG.info("Opening srcFile form offset:" + offset/1024/1024 +
							" for location " + locationIndex);
					FSDataInputStream s = fs.open(
							srcFile, conf.getInt("io.file.buffer.size", 64 * 1024));
					s.seek(offset);
					return s;
				}
			}
		}
	}

	public LRC1Decoder(Configuration conf) {
		super(conf, Codec.getCodec("lrc1"));
		xorCodec = Codec.getCodec("lrc1_xor");
		lrc1Code = (LRC1Code)code;
	}

	@Override
	long fixErasedBlockImpl(FileSystem srcFs, Path srcFile, FileSystem parityFs,
							Path parityFile, boolean fixSource, long blockSize,
							long errorOffset, long limit, boolean partial,
							OutputStream out, Progressable reporter, CRC32 crc) throws IOException {
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
			lp = getBlockLocation(srcFs, srcFile, blockIdx, conf);
			// 当前坏块在 strip中的索引(parity块在前)
			erasedLocationToFix = 2 + lp.getBlockIdxInStripe();
		} else {
			lp = getParityBlockLocation(blockIdx);
			erasedLocationToFix = lp.getBlockIdxInStripe();
		}
		erasedLocations = new ArrayList<Integer>();
		locationsToRead = new ArrayList<Integer>(xorCodec.parityLength + xorCodec.stripeLength);
		erasedLocations.add(erasedLocationToFix);
		LOG.info("erasedLocation : "+erasedLocationToFix+" in stripe " + lp.getStripeIdx());


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
						StripeReader sReader = new LRC1StripeReader(conf, blockSize,
										xorCodec, srcFs, lp.getStripeIdx(),
										srcStat.getPath(), srcStat.getLen());
						// 创建输入流,更新erasedLocations
						inputs = sReader.buildInputs(srcFs, srcFile,
								srcStat, parityFs, parityFile, parityStat,
								lp.getStripeIdx(), offsetInBlock, erasedLocations,
								locationsToRead,lrc1Code.getXor());
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
					lrc1Code.getXor().decodeBulk(readResult.readBufs, writeBufs, erasedLocationsArray,
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
						logRaidReconstructionMetrics("FAILURE", 0, xorCodec,
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
			logRaidReconstructionMetrics("SUCCESS", written, xorCodec,
					System.currentTimeMillis() - startTime,
					erasedLocations.size(), numReadBytes, srcFile, errorOffset,
					RaidNode.LOGTYPES.OFFLINE_RECONSTRUCTION, srcFs);
			return written;
		}catch (Exception e){
			e.printStackTrace();
		} finally {
			if (parallelReader != null) parallelReader.shutdown();
			RaidUtils.closeStreams(inputs);
			return 0;
		}
	}

	private StripeReader.LocationPair getBlockLocation(FileSystem srcFs,
													  Path srcFile, int blockIdxInFile, Configuration conf)
			throws IOException {
		int stripeIdx = 0;
		int blockIdxInStripe = 0;
		int blockIdx = blockIdxInFile;
		List<FileStatus> lfs = null;
		if (codec.isDirRaid) {
			Path parentPath = srcFile.getParent();
			lfs = RaidNode.listDirectoryRaidFileStatus(conf, srcFs, parentPath);
			if (lfs == null) {
				throw new IOException("Couldn't list files under " + parentPath);
			}
			int blockNum = 0;
			Path qSrcFile = srcFs.makeQualified(srcFile);
			for (FileStatus fsStat : lfs) {
				if (!fsStat.getPath().equals(qSrcFile)) {
					blockNum += RaidNode.getNumBlocks(fsStat);
				} else {
					blockNum += blockIdxInFile;
					break;
				}
			}
			blockIdx = blockNum;
		}
		stripeIdx = blockIdx / 3;
		blockIdxInStripe = blockIdx % 3;
		return new StripeReader.LocationPair(stripeIdx, blockIdxInStripe, lfs);
	}

	private StripeReader.LocationPair getParityBlockLocation(final int blockIdxInFile) {

		int stripeIdx = blockIdxInFile / 2;
		int blockIdxInStripe = blockIdxInFile % 2;

		return new StripeReader.LocationPair(stripeIdx, blockIdxInStripe, null);
	}
	private void fillLocations(){
		int i = 0;
		erasedLocationsArray = new int[erasedLocations.size()];
		for (int loc = 0; loc < xorCodec.stripeLength + xorCodec.parityLength;
			 loc++) {
			if (erasedLocations.indexOf(loc) >= 0) {
				erasedLocationsArray[i] = loc;
				i++;
			}
		}
		i = 0;
		locationsToReadArray = new int[locationsToRead.size()];
		for (int loc = 0; loc < xorCodec.stripeLength + xorCodec.parityLength;
			 loc++) {
			if (locationsToRead.indexOf(loc) >= 0) {
				locationsToReadArray[i] = loc;
				i++;
			}
		}
		i = 0;
		locationsNotToReadArray = new int[xorCodec.stripeLength +
				xorCodec.parityLength -
				locationsToRead.size()];
		for (int loc = 0; loc < xorCodec.stripeLength + xorCodec.parityLength;
			 loc++) {
			if (locationsToRead.indexOf(loc) == -1 ||
					erasedLocations.indexOf(loc) != -1) {
				locationsNotToReadArray[i] = loc;
				i++;
			}
		}

	}
}
