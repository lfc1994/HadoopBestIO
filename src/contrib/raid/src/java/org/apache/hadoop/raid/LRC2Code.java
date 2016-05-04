package org.apache.hadoop.raid;

import java.util.*;

/**
 * Created by JackJay on 16/4/28.
 */
public class LRC2Code extends ErasureCode{
	private int stripLen;
	private static final int GROUP_NUM = 2;
	private int parityLen;
	private ReedSolomonCode rsCode = null;
	@Override
	public void encode(int[] message, int[] parity) {

	}

	@Override
	public void decode(int[] data, int[] erasedLocations, int[] erasedValues) {

	}

	@Override
	public void decode(int[] data, int[] erasedLocations, int[] erasedValues, int[] locationsToRead, int[] locationsNotToRead) {

	}

	@Override
	public int stripeSize() {
		return stripLen;
	}

	@Override
	public int paritySize() {
		return parityLen;
	}

	@Override
	public void init(Codec codec) {
		this.stripLen = codec.stripeLength;
		this.parityLen = codec.parityLength;
		Codec rs = CodeUtil.createInnerCodec("lrc2_rs","org.apache.hadoop.raid.ReedSolomonCode",6,2);
		rsCode = new ReedSolomonCode();
		rsCode.init(rs);

	}

	@Override
	public int symbolSize() {
		return 0;
	}

	@Override
	public void encodeBulk(byte[][] inputs, byte[][] outputs) {
		byte[][] tmpOut = new byte[2][];
		byte[][] tmpIn = new byte[stripLen][];

		// global parity
		tmpOut[0] = outputs[0];
		tmpOut[1] = outputs[1];
		rsCode.encodeBulk(inputs,tmpOut);

		// local parity one
		for(int i = 0;i < inputs.length;i++){
			if(i < 3) tmpIn[i] = inputs[i];
			else Arrays.fill(tmpIn[i],(byte)0);
		}
		tmpOut[0] = outputs[2];
		Arrays.fill(tmpOut[2],(byte)0);
		rsCode.encodeBulk(inputs,tmpOut);

		// local parity two
		for(int i = 0;i < inputs.length;i++){
			if(i >= 3) tmpIn[i] = inputs[i];
			else Arrays.fill(tmpIn[i],(byte)0);
		}
		Arrays.fill(tmpOut[0],(byte)0);
		tmpOut[1] = outputs[3];
		rsCode.encodeBulk(inputs,tmpOut);
	}

	// 正向和逆向的区别
	// 6,2,2 lrc 目前只处理一个擦除
	@Override
	public List<Integer> locationsToReadForDecode(List<Integer> erasedLocations) throws TooManyErasedLocations {
		List<Integer> locationsToRead = new ArrayList<Integer>();

		// 错误个数太多时抛出异常
		if (erasedLocations.size() > 1) {
			String locationsStr = "";
			for (Integer erasedLocation : erasedLocations) {
				locationsStr += " " + erasedLocation;
			}
			throw new TooManyErasedLocations("Locations " + locationsStr);
		}else if(erasedLocations.size() == 1){
			int erased = erasedLocations.get(0);

			// 第一组源数据
			int srcIdx = erased-parityLen;
			if(srcIdx >= 0 && srcIdx < 3){
				locationsToRead.add(2);// 第一个局部校验块
				for(int i = 0;i < 3;i++){
					if(srcIdx != i) erasedLocations.add(srcIdx+parityLen);
				}
			}
			// 第二组源数据
			else if(srcIdx >= 3){
				locationsToRead.add(3);// 第二个局部校验块
				for(int i = 3;i < 6;i++){
					if(srcIdx != i) erasedLocations.add(srcIdx+parityLen);
				}
			}
			// 校验块
			else{
				switch (erased){
					case 0:
						locationsToRead.add(2);
					case 3:
						for(int i = 3;i < 6;i++) locationsToRead.add(i+parityLen);
						break;
					case 1:
						locationsToRead.add(3);
					case 2:
						for(int i = 0;i < 3;i++) locationsToRead.add(i+parityLen);
						break;

				}
			}
		}
		return locationsToRead;
	}

	@Override
	public void decodeBulk(byte[][] readBufs, byte[][] writeBufs,
						   int[] erasedLocations, int[] locationsToRead,
						   int[] locationsNotToRead) {
		// 只处理1个擦除
		int erasedLoc = erasedLocations[0];
		int srcIdx = erasedLoc-parityLen;
		int bufSize = readBufs[0].length;

		// 源数据块修复
		if(srcIdx >= 0 && srcIdx < 6){
			// (6,2)rs码解码输入
			byte[][] tmpIn = new byte[8][];
			// 不使用全局校验
			for(int i = 0;i < 8;i++){
				tmpIn[i] = readBufs[i+2];
			}
			rsCode.decodeBulk(tmpIn,writeBufs,new int[]{erasedLoc});
		}
		// 全局校验块修复
		else if(erasedLoc == 0 || erasedLoc == 1){
			// 计算第二组的rs编码结果
			byte[][] tmpIn = new byte[6][];
			for(int i = 0;i < 6;i++){
				tmpIn[i] = readBufs[i+4];
			}

			byte[][] tmpOut = new byte[2][];
			if(erasedLoc == 0){// 第一个校验块
				tmpOut[0] = writeBufs[0];
				tmpOut[1] = new byte[bufSize];
			}else{// 第二个校验块
				tmpOut[0] = new byte[bufSize];
				tmpOut[1] = writeBufs[0];
			}
			rsCode.encodeBulk(tmpIn,tmpOut);

			// 与相对应的局部校验块相加
			int localIdx = (erasedLoc == 0 ? 2 : 3);
			for(int i = 0;i < bufSize;i++){
				writeBufs[0][i] ^= readBufs[localIdx][i];
			}
		}
		// 局部校验块修复
		else if(erasedLoc == 2 || erasedLoc == 3){
			byte[][] tmpIn = new byte[6][];
			for(int i = 0;i < 6;i++){
				tmpIn[i] = readBufs[i+4];
			}

			byte[][] tmpOut = new byte[2][];
			if(erasedLoc == 2){// 第一个校验块
				tmpOut[0] = writeBufs[0];
				tmpOut[1] = new byte[bufSize];
			}else{// 第二个校验块
				tmpOut[0] = new byte[bufSize];
				tmpOut[1] = writeBufs[0];
			}
			rsCode.encodeBulk(tmpIn,tmpOut);
		}

	}
}
