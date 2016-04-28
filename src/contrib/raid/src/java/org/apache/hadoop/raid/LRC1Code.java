package org.apache.hadoop.raid;


import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by JackJay on 16/4/24.
 * 基于(6,2)RS编码和(4,1)xor编码的一种局部修复编码策略
 * 本LRC码为(6,4)
 */
public class LRC1Code extends ErasureCode {
	private int stripeLen;
	private int parityLen;

	// (6,2)rs
	private ReedSolomonCode rs = null;
	// (4,1)xor
	private XORCode xor = null;

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
	public List<Integer> locationsToReadForDecode(List<Integer> erasedLocations) throws TooManyErasedLocations {
		if(erasedLocations.size() != 1)
			throw new TooManyErasedLocations("current best_io code can only repair 1 corrupt");

		List<Integer> locationsToRead = new ArrayList<Integer>(stripeSize());
		int limit = stripeSize() + paritySize();
		for (int loc = limit - 1; loc >= 0; loc--) {
			if (erasedLocations.indexOf(loc) == -1) {
				locationsToRead.add(loc);
			}
		}
		return  locationsToRead;
	}

	@Override
	public int stripeSize() {
		return stripeLen;
	}

	@Override
	public int paritySize() {
		return parityLen;
	}

	@Override
	public void init(Codec codec) {
		this.stripeLen = codec.stripeLength;
		this.parityLen = codec.parityLength;
		this.rs = new ReedSolomonCode();
		rs.init(Codec.getCodec("lrc1_rs"));
		this.xor = new XORCode();
		xor.init(Codec.getCodec("lrc1_xor"));
	}

	@Override
	public void encodeBulk(byte[][] inputs, byte[][] outputs) {
		assert (inputs.length == 6 && outputs.length == 4);
		byte tmpOut[][] = new byte[2][];
		byte tmpIn[][] = new byte[4][];
		byte[][] tmpRs = new byte[6][inputs[0].length];
		for(int i  = 0;i < inputs.length;i++){
			System.arraycopy(inputs[i],0,tmpRs[i],0,inputs[0].length);
		}
		// (6,2)rs encode
		tmpOut[0] = outputs[0];
		tmpOut[1] = outputs[2];
		// rs 编码结束后输入数组会被清空
		rs.encodeBulk(tmpRs,tmpOut);

		// (4,1)xor ecnode
		tmpIn[0] = inputs[0];
		tmpIn[1] = inputs[1];
		tmpIn[2] = inputs[2];
		tmpIn[3] = outputs[0];
		tmpOut[0] = outputs[1];
		xor.encodeBulk(tmpIn,tmpOut);

		tmpIn[0] = inputs[3];
		tmpIn[1] = inputs[4];
		tmpIn[2] = inputs[5];
		tmpIn[3] = outputs[2];
		tmpOut[0] = outputs[3];
		xor.encodeBulk(tmpIn,tmpOut);
	}

	private void printMatrix(byte[][] matrix,String name,String line){
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

	public void test(byte[][] inputs, byte[][] outputs){
		assert (inputs.length == 6 && outputs.length == 4);
		byte tmpOut[][] = new byte[2][];
		byte tmpIn[][] = new byte[4][];
		byte[][] tmpRs = new byte[6][inputs[0].length];
		for(int i  = 0;i < inputs.length;i++){
			System.arraycopy(inputs[i],0,tmpRs[i],0,inputs[0].length);
		}
		// (6,2)rs encode
		tmpOut[0] = outputs[0];
		tmpOut[1] = outputs[2];
		rs.encodeBulk(tmpRs, tmpOut);
		printMatrix(outputs,"rsOut","P");

		// (4,1)xor ecnode
		tmpIn[0] = inputs[0];
		tmpIn[1] = inputs[1];
		tmpIn[2] = inputs[2];
		tmpIn[3] = outputs[0];
		tmpOut[0] = outputs[1];
		xor.encodeBulk(tmpIn,tmpOut);
		printMatrix(tmpIn, "tmpIn 1", "D");
		printMatrix(outputs,"out 1","P");

		tmpIn[0] = inputs[3];
		tmpIn[1] = inputs[4];
		tmpIn[2] = inputs[5];
		tmpIn[3] = outputs[2];
		tmpOut[0] = outputs[3];
		xor.encodeBulk(tmpIn,tmpOut);
		printMatrix(tmpIn, "tmpIn 2", "D");
		printMatrix(outputs,"out 2","P");
	}

	@Override
	public int symbolSize() {
		return 0;
	}

	public XORCode getXor() {
		return xor;
	}
}
