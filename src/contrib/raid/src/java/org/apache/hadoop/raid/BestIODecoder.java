package org.apache.hadoop.raid;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.Progressable;

import java.awt.*;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.zip.CRC32;

/**
 * Created by JackJay on 16/4/12.
 */
public class BestIODecoder extends Decoder{
	public static final Log LOG = LogFactory.getLog("BestIODecoder");
	private int[] erasedLocationsArray = null;
	private int[] locationsToReadArray = null;
	private int[] locationsNotToReadArray = null;
	private List<Integer> erasedLocations = null;
	private List<Integer> locationsToRead = null;
	private BestIOCode bestIOCode = null;

	private static class DecodeTmpHelper extends BestIOEncoder.TmpFilesHelper{
		public DecodeTmpHelper(Configuration conf) {
			super(conf);
		}

		// offset 无用,只是因为重写
		@Override
		public void writeTmpFiles(byte[][] data,int offset) {
			int bufLength = data[0].length;
			try {
				for(int i = 0;i < data.length;i++){
					tmpFilesOut[i].write(data[i], 0, bufLength);
				}
			} catch (IOException e) {
				LOG.error("write tmp data failed!");
			}
		}
	}
	class BestIOParallelReader extends ParallelStreamReader{
		int erasedBlockInStripe;
		long startOffsets[];
		long maxBytesPerStream;

		private class MainThread extends Thread{
			@Override
			public void run() {
				initStartOffset();
				while (running) {
					ReadResult readResult = new ReadResult(streams.length, bufSize);
					try {
						if (remainingBytesPerStream == 0) {
							return;
						}
						// 在各个线程读之前,根据downMap调整输入流的SeekOffset
						seekStreams((int) (maxBytesPerStream - remainingBytesPerStream) / bufSize);
						performReads(readResult);
						// Enqueue to bounder buffer.
						boundedBuffer.put(readResult);
						remainingBytesPerStream -= bufSize;
					} catch (InterruptedException e) {
						running = false;
					}
				}
			}
			private void initStartOffset(){
				FSDataInputStream fsIn = null;
				RaidUtils.ZeroInputStream zeroIn = null;
				System.out.print("startOffsets:");
				for(int i = 0;i < streams.length;i++){
					if(streams[i] instanceof FSDataInputStream){
						fsIn = (FSDataInputStream)streams[i];
						try {
							startOffsets[i] = fsIn.getPos();
						} catch (IOException e) {
							System.out.print("init startOffset error : get pos failed");
						}
						System.out.print(startOffsets[i]/1024/1024/64+",");
					}
				}
				System.out.println();
			}
			private void seekStreams(int downBufIndex){
				FSDataInputStream in = null;
				RaidUtils.ZeroInputStream zeroIn = null;
				long level = BestIOCode.downMap[erasedBlockInStripe][downBufIndex];
				for(int i = 0;i < streams.length;i++){
					if(streams[i] == null) {
						LOG.error("stream[i] is null!!");
						continue;
					}
					else if(streams[i] instanceof FSDataInputStream){
						in = (FSDataInputStream)streams[i];
						if(in == null) LOG.error(" cast null!! ");
						try {
							// pos会变化,pos是流在文件中的偏移
							in.seek(startOffsets[i]+(level-1)*bufSize);
							endOffsets[i] = in.getPos()+bufSize;
							LOG.info("myErasedLoc is "+(erasedBlockInStripe+1)+",streams["+i+"] seeked to level "+level);
						} catch (IOException e) {
							LOG.error("FSDataInputStream seek error!!!\n");
						}
					}
				}
			}
		}


		public BestIOParallelReader(Progressable reporter, InputStream[] streams,
									int erasedBlockInStripe,int bufSize, long maxBytesPerStream){
			this.reporter = reporter;
			this.streams = new InputStream[streams.length];
			for(int i = 0;i < streams.length;i++){
				this.streams[i] = streams[i];
			}
			this.erasedBlockInStripe = erasedBlockInStripe;
			this.startOffsets = new long[streams.length];
			this.endOffsets = new long[streams.length];
			this.boundedBuffer = new ArrayBlockingQueue<ParallelStreamReader.ReadResult>(2);
			this.numThreads = streams.length;
			this.bufSize = bufSize;
			this.maxBytesPerStream = maxBytesPerStream;
			this.remainingBytesPerStream = maxBytesPerStream;
			this.slots = new Semaphore(this.numThreads);
			this.readPool = Executors.newFixedThreadPool(this.numThreads);
			this.mainThread = new MainThread();
		}

		private void performReads(ReadResult readResult)
				throws InterruptedException {
			long start = System.currentTimeMillis();
			// 为每一个输入流创建一个ReadOperation线程读取数据
			for (int i = 0; i < streams.length; ) {
				boolean acquired = slots.tryAcquire(1, 10, TimeUnit.SECONDS);
				reporter.progress();
				if (acquired) {
					readPool.execute(new ReadOperation(readResult, i));
					i++;
				}
			}
			// All read operations have been submitted to the readPool.
			// Now wait for the operations to finish and release the semaphore.
			while (true) {
				boolean acquired =
						slots.tryAcquire(numThreads, 10, TimeUnit.SECONDS);
				reporter.progress();
				if (acquired) {
					slots.release(numThreads);
					break;
				}
			}

			readTime += (System.currentTimeMillis() - start);
		}
	}

	public BestIODecoder(Configuration conf) {
		super(conf,Codec.getCodec("best_io"));
		this.bufSize = 8 * 1024 * 1024;
		bestIOCode = (BestIOCode)code;
	}

	// 当最后一个文件块内容长度小于blockSize时存在limit 有关的bug
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
		int erasedLocationToFix,myErasedLoc;
		//一个条带中,校验block在数据block之前.计算起始错误块在一个条带中的位置索引
		if (fixSource) {
			// 计算当前坏块所在stripe,以locationPair数据结构记录
			lp = StripeReader.getBlockLocation(codec, srcFs, srcFile, blockIdx, conf);
			// 当前坏块在 strip中的索引(parity块在前)
			erasedLocationToFix = codec.parityLength + lp.getBlockIdxInStripe();
			myErasedLoc = erasedLocationToFix -codec.parityLength;
		} else {
			lp = StripeReader.getParityBlockLocation(codec, blockIdx);
			erasedLocationToFix = lp.getBlockIdxInStripe();
			myErasedLoc = erasedLocationToFix+codec.stripeLength;
		}
		erasedLocations = new ArrayList<Integer>();
		locationsToRead = new ArrayList<Integer>(codec.parityLength + codec.stripeLength);
		erasedLocations.add(erasedLocationToFix);
		System.out.printf("Need to write %d Mb for erasedLoc %d in stripe %d\n",
				limit/1024/1024,erasedLocationToFix,lp.getStripeIdx());

		/**
		 * 初始化其他变量
		 */
		FileStatus srcStat = srcFs.getFileStatus(srcFile);
		FileStatus parityStat = parityFs.getFileStatus(parityFile);
		InputStream[] inputs = null;
		BestIOParallelReader parallelReader = null;
		long startOffsetInBlock = 0;
		if (partial) startOffsetInBlock = errorOffset % blockSize;
		int numReadBytes = 0;
		long written;
		DecodeTmpHelper helper = new DecodeTmpHelper(conf);
		helper.initTmpFiles("decode",5);

		try {
			for (int toDown = 0; toDown < blockSize/2;toDown+=bufSize) {
				try {

					/**
					 * 创建并启动并行流读取器
					 */
					if (parallelReader == null) {
						long offsetInBlock = toDown + startOffsetInBlock;
						StripeReader sReader = StripeReader.getStripeReader(codec,
								conf, blockSize, srcFs, lp.getStripeIdx(), srcStat);
						// 创建输入流,更新erasedLocations
						inputs = sReader.buildInputs(srcFs, srcFile,
								srcStat, parityFs, parityFile, parityStat,
								lp.getStripeIdx(), offsetInBlock, erasedLocations,
								locationsToRead, bestIOCode);
						LOG.info("Erased locations: " + erasedLocations.toString() +
								"\nLocations to Read for repair:" +
								locationsToRead.toString());

						// 启动parallReader
						this.writeBufs = new byte[erasedLocations.size()][];
						for (int i = 0; i < writeBufs.length; i++) {
							writeBufs[i] = new byte[bufSize];
						}
						parallelReader = new BestIOParallelReader(reporter, inputs,myErasedLoc,
								bufSize, blockSize/2);
						parallelReader.start();
					}

					/**
					 * 读取帮助数据并解码,最终写入临时块文件
					 */
					// read过程中捕获到的异常将会抛出,并添加错误位置到erasedLocations
					ParallelStreamReader.ReadResult readResult = readFromInputs(
							erasedLocations, bufSize, reporter, parallelReader);
					fillLocations();

					// 解码并保存所有数据到临时文件
					bestIOCode.decodeDown(readResult.readBufs, writeBufs[0], erasedLocationsArray);
					byte[][] ttmp = new byte[5][];
					for(int i = 0;i < 3;i++){
						if(i == myErasedLoc){
							ttmp[i] = writeBufs[0];
							continue;
						}
						ttmp[i] = readResult.readBufs[i+2];
					}
					ttmp[3] = readResult.readBufs[0];// p1
					ttmp[4] = readResult.readBufs[1];// p2

					helper.writeTmpFiles(ttmp,(int)toDown);

					// 打印信息,调试用
					System.out.println("download and recover level "
							+bestIOCode.downMap[myErasedLoc][toDown/bufSize]+":");
					for(int i = 0;i < 5;i++){
						if(i == myErasedLoc) System.out.printf("recovered [%d]:",i);
						else System.out.printf("downloaded [%d]:",i);
						for(int j = 0;j < 5;j++){
							System.out.printf("%02x,", ttmp[i][j]);
						}
						System.out.println();
					}
					System.out.println();


					// 更新numReadBytes
					for (int readNum : readResult.numRead) numReadBytes += readNum;
				} catch (IOException e) {
					if (e instanceof TooManyErasedLocations) {
						logRaidReconstructionMetrics("FAILURE", 0, codec,
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
			LOG.info("Start to read Tmp file and recover remained data");
			// 关闭临时文件输出流
			helper.closeTmpFilesOut();
			boolean recovered;
			byte[][] neededData = null;
			byte[] recoveredData = new byte[bufSize];
			int i = 0;
			for(written = 0;written < limit;){
				i =(int) written/bufSize;
				recovered = false;
				for(int j = 0;j < 4;j++){
					if(bestIOCode.downMap[myErasedLoc][j] == i+1) {
						System.out.printf("Recover level %d from tmp file %d offset %d*8M\n",i+1, myErasedLoc, j);
						recovered = true;
						helper.tmpFilesIn[myErasedLoc].seek(j * bufSize);
						helper.tmpFilesIn[myErasedLoc].read(recoveredData, 0, bufSize);
						break;
					}
				}
				if(!recovered){
					System.out.println("Recover level "+(i+1));
					for(Point[] ps : bestIOCode.recoverMap){
						// (损坏节点,第几层)
						if(ps[0].x-1 == myErasedLoc && ps[0].y-1 == i){
							neededData = new byte[ps.length-1][bufSize];
							for(int k = 1;k < ps.length;k++){
								for(int m = 0;m < 4;m++){
									if(bestIOCode.downMap[myErasedLoc][m] == ps[k].y){
										System.out.printf("reading node %d level %d from tmpfile %d offset %d*8M\n",
												ps[k].x,ps[k].y,ps[k].x-1,m);
										helper.tmpFilesIn[ps[k].x-1].seek(m * bufSize);
										helper.tmpFilesIn[ps[k].x-1].read(neededData[k-1], 0, bufSize);
									}
								}
							}
							bestIOCode.decodeHalf(neededData, recoveredData);
							break;
						}
					}
				}

				// 使用输出流out写解码数据,并更新crc
				int toWrite = (int) Math.min((long) bufSize, limit - written);
				if (out != null)
					out.write(recoveredData,0,toWrite);
				if (crc != null) {
					crc.update(recoveredData, 0, toWrite);
				}
				written += toWrite;
				System.out.printf("write recovered level %d firstByte: 0x%02x\n\n",(i+1),recoveredData[0]);
			}
			helper.closeTmpFilesIn();
			helper.removeTmpFiles();
			//更新raid重构监控数据
			logRaidReconstructionMetrics("SUCCESS", written, codec,
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
		for (int loc = 0; loc < codec.stripeLength + codec.parityLength;loc++) {
			if (erasedLocations.indexOf(loc) >= 0) {
				erasedLocationsArray[i] = loc;
				i++;
			}
		}
		i = 0;
		locationsToReadArray = new int[locationsToRead.size()];
		for (int loc = 0; loc < codec.stripeLength + codec.parityLength;
			 loc++) {
			if (locationsToRead.indexOf(loc) >= 0) {
				locationsToReadArray[i] = loc;
				i++;
			}
		}
		i = 0;
		locationsNotToReadArray = new int[codec.stripeLength +
				codec.parityLength -
				locationsToRead.size()];
		for (int loc = 0; loc < codec.stripeLength + codec.parityLength;
			 loc++) {
			if (locationsToRead.indexOf(loc) == -1 ||
					erasedLocations.indexOf(loc) != -1) {
				locationsNotToReadArray[i] = loc;
				i++;
			}
		}

	}
}
