package org.apache.hadoop.raid;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by JackJay on 16/4/27.
 */
public class CodeTestUtils {
	public static final Log LOG = LogFactory.getLog("CodeTestUtils : ");
	private static final long BLOCK_SIZE = 64 * 1024 * 1024;
	private static final int BUFF_LEN = 5;
	private Configuration conf = null;
	private Path src = null;
	private Path parity = null;
	private Codec codec = null;
	private ErasureCode code = null;
	private BestIOCode bestIOCode = null;
	private LRC1Code lrc1Code = null;

	private FileSystem srcFs = null;
	private FileSystem parityFs = null;
	private FSDataInputStream srcIn = null;
	private FSDataInputStream parityIn = null;
	private byte[][] srcBuff = new byte[codec.stripeLength][BUFF_LEN];
	private byte[][] newSrcBuff = new byte[codec.stripeLength][BUFF_LEN];
	private byte[][] newParityBuff = new byte[codec.parityLength][BUFF_LEN];
	private byte[][] parityBuff = new byte[codec.parityLength][BUFF_LEN];

	public CodeTestUtils(String[] args,int startIndex,Configuration conf) {
		this.conf = conf;
		src = new Path(args[startIndex]);
		parity = new Path(args[startIndex + 1]);
		codec = Codec.getCodec(args[startIndex + 2]);
		code = codec.createErasureCode(conf);
		if(code instanceof BestIOCode) bestIOCode = (BestIOCode)code;
		LOG.info("Testing src " + src + " and parity " + parity + " using " + codec);

	}

	public static void printMatrix(byte[][] matrix,String name,String line){
		System.out.println(name);
		for(int i = 0;i < matrix.length;i++){
			System.out.print(line+i+" : ");
			for(int j = 0;j < matrix[0].length;j++){
				System.out.printf("%02x ",matrix[i][j]);
			}
			System.out.println();
		}
		System.out.println();
	}

	private void initStream(){
		try {
			srcFs = src.getFileSystem(conf);
			parityFs = parity.getFileSystem(conf);
			srcIn = srcFs.open(src, conf.getInt("io.file.buffer.size", 64 * 1024));
			parityIn = parityFs.open(parity, conf.getInt("io.file.buffer.size", 64 * 1024));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	private void downloadData() throws IOException{
		for(int i = 0;i < codec.stripeLength + codec.parityLength;i++){
			if(i < codec.stripeLength){// 源文件数据块
				srcIn.seek(i*BLOCK_SIZE);
				srcIn.read(srcBuff[i]);
			}else {// 校验块
				parityIn.seek((i-codec.stripeLength)*BLOCK_SIZE);
				parityIn.read(parityBuff[i-codec.stripeLength]);
			}
		}
	}
	private void testEncode(){
		System.out.println("Test Encoding for codec " + codec.id);
		try {
			downloadData();
		} catch (IOException e) {
			e.printStackTrace();
		}
		printMatrix(srcBuff, "src data", "D");

		switch (codec.id){
			case "best_io":
				bestIOCode.encodeSingle(srcBuff, newParityBuff[0]);
				break;
			default:
				code.encodeBulk(srcBuff, newParityBuff);
		}

		printMatrix(newParityBuff, "new Parity data", "P");
		printMatrix(parityBuff, "origin Parity data", "P");
	}

	private void testDecode(){
		System.out.println("Test Decoding for codec "+codec.id);

	}

	// 第一个条带中的每一个快的前5个字节.
	public void test(){
		testEncode();
	}
}
