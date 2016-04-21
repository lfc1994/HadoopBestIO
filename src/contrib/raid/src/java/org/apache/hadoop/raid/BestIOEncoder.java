package org.apache.hadoop.raid;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Progressable;

import java.awt.*;
import java.io.*;

/**
 * Created by JackJay on 16/4/18.
 */
public class BestIOEncoder extends Encoder {
	private BestIOCode bestCode = null;

	public static class TmpFilesHelper{
		protected File[] tmpFiles = null;
		protected OutputStream[] tmpFilesOut = null;
		protected RandomAccessFile[] tmpFilesIn = null;
		protected static String tmpPrefix = null;

		public TmpFilesHelper(Configuration conf) {
			tmpPrefix = conf.get("dfs.tmp.dir");
		}

		public boolean initTmpFiles(String fileNamePrefix,int amount){
			boolean isSuccess = true;
			// 创建临时文件
			tmpFiles = new File[amount];
			tmpFilesOut = new OutputStream[amount];
			tmpFilesIn = new RandomAccessFile[amount];

			for(int i = 0;i < tmpFiles.length;i++){
				tmpFiles[i] = new File(tmpPrefix+"/bestIOtmp_"+fileNamePrefix+i);
				try {
					tmpFiles[i].createNewFile();
					tmpFilesOut[i] = new FileOutputStream(tmpFiles[i]);
					tmpFilesIn[i] = new RandomAccessFile(tmpFiles[i],"r");
					//LOG.info("init tmp files:"+ tmpFiles[i].getAbsolutePath());
				} catch (FileNotFoundException e){
					LOG.error("tmp files not found!");
					isSuccess = false;
				} catch (IOException e) {
					LOG.error("cannot create tmp file:"+ tmpFiles[i].getAbsolutePath());
					isSuccess = false;
				}
			}
			return isSuccess;
		}
		// 保存下载数据
		public void writeTmpFiles(byte[][] data,int offset){
			int bufLength = data[0].length;
			try {
				for(int i = 0;i < data.length;i++){
					tmpFilesOut[i].write(data[i], 0, bufLength);
				}
//				// d3
//				if(offset/bufLength >= 4) tmpFilesOut[2].write(data[2],0,bufLength);
//				// p1
//				if(offset/bufLength < 4) tmpFilesOut[3].write(data[3],0,bufLength);

			} catch (IOException e) {
				LOG.error("write tmp data failed!");
			}
		}

		public void closeTmpFilesIn(){
			for(int i = 0;i < tmpFilesIn.length;i++){
				try {
					tmpFilesIn[i].close();
				} catch (IOException e) {
					LOG.error("close tmp files random access failed!");
				}finally {
					tmpFilesIn[i] = null;
				}
			}
		}
		public void closeTmpFilesOut(){
			for(int i = 0;i < tmpFilesOut.length;i++){
				try {
					tmpFilesOut[i].close();
					tmpFilesOut[i] = null;
				} catch (IOException e) {
					LOG.error("close tmp file output stream failed!");
				}finally {
					tmpFilesOut[i] = null;
				}
			}
		}
		public void removeTmpFiles(){
			for(int i = 0;i < tmpFiles.length;i++){
				tmpFiles[i].delete();
				tmpFiles[i] = null;
			}
		}

		public RandomAccessFile[] getTmpFilesIn() {
			return this.tmpFilesIn;
		}

	}

	public BestIOEncoder(Configuration conf) {
		super(conf, Codec.getCodec("best_io"));
		bestCode = (BestIOCode)code;
		this.bufSize = 8 * 1024 * 1024;
	}
	// 将块分为8层
	private void configureBuffers(long blockSize){
		bufSize = (int)blockSize/8;
		for(int i = 0;i < bestCode.paritySize();i++){
			writeBufs[i] = new byte[bufSize];
		}
	}

	@Override
	void encodeStripe(
			InputStream[] blocks,
			long blockSize,
			OutputStream[] outs,
			Progressable reporter) throws IOException {

		configureBuffers(blockSize);
		// 创建临时文件并初始化输入输出流
		TmpFilesHelper helper = new TmpFilesHelper(conf);
		if(!helper.initTmpFiles("encode",4)) {
			throw new IOException("cannot init tmp files to save download data for encode p2!");
		}

		ParallelStreamReader parallelReader = new ParallelStreamReader(
				reporter, blocks, bufSize,
				parallelism, 1, blockSize);
		parallelReader.start();

		// 读取全部数据并编码P1,同时保存编码P2所需要的数据,并编码P2
		try {
			for (long encoded = 0; encoded < blockSize; encoded += bufSize) {
				ParallelStreamReader.ReadResult readResult = null;
				try {
					readResult = parallelReader.getReadResult();
				} catch (InterruptedException e) {
					throw new IOException("读取等待被中断!");
				}
				// 编码过程不能容忍IO错误
				IOException readEx = readResult.getException();
				if (readEx != null) throw readEx;

				// 先编码P1
				bestCode.encodeSingle(readResult.readBufs, writeBufs[0]);
				reporter.progress();
				// 将P1数据写入临时块文件
				try {
					outs[0].write(writeBufs[0], 0, bufSize);
				}catch (IOException e){
					LOG.error("write to p1 block file failed!");
				}
				reporter.progress();

				// 保存下载数据
				byte[][] ttmp = new byte[4][];
				for(int i = 0;i < 3;i++){
					ttmp[i] = readResult.readBufs[i];
				}
				ttmp[3] = writeBufs[0];

				// 打印encode p1部分信息
				int level = (int)encoded/bufSize;
				level ++;
				if(level == 1){
					System.out.print("level 1 5bytes p1 : ");
					for(int j = 0;j < 5;j++){
						System.out.print(writeBufs[0][j]+",");
					}
					System.out.println();
				}
				if(level == 2 || level == 3 || level == 5){
					for(int j = 0;j <5;j++){
						switch (level){
							case 2:System.out.print("node (1,2) :"+ttmp[0][j]+",");
								break;
							case 3:System.out.print("node (2,3) :"+ttmp[1][j]+",");
								break;
							case 5:System.out.print("node (3,5) :"+ttmp[2][j]+",");
								break;
						}
					}
					System.out.println();
				}
				helper.writeTmpFiles(ttmp, (int) encoded);
			}
		} finally {
			parallelReader.shutdown();
			// 关闭输出流
			helper.closeTmpFilesOut();
		}

		// 编码P2
		try {
			Point[] points = null;
			Point p = null;
			RandomAccessFile[] tmpIn = helper.getTmpFilesIn();
			for(int i = 0;i < bestCode.encodeMap.size();i++){
				points = bestCode.encodeMap.get(i);
				readBufs = new byte[points.length][bufSize];
				for(int j = 0;j < points.length;j++){
					p = points[j];
					tmpIn[p.x-1].seek((p.y-1)*bufSize);
					tmpIn[p.x-1].read(readBufs[j], 0, bufSize);

					//打印一组信息
					if(i == 0){
						System.out.print("down ("+p.x+","+p.y+") : ");
						for(int k = 0;k < 5;k++){
							System.out.print(readBufs[j][k]+",");
						}
						System.out.println();
					}
				}
				//编码P2
				bestCode.encodeSingle(readBufs,writeBufs[1]);
				outs[1].write(writeBufs[1], 0, bufSize);
			}
		}finally {
			helper.closeTmpFilesIn();
			helper.removeTmpFiles();
		}
	}
}
