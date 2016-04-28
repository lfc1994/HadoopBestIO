package org.apache.hadoop.raid;

import java.awt.Point;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 最佳IO编码算法实现类
 */
public class BestIOCode extends ErasureCode{

	public static final List<Point[]> encodeMap = new ArrayList<Point []>();
	// d1,d2,d3,p1
	public static final int [][] downMap = {{1,3,5,7},{1,2,5,6},{1,2,3,4},{5,6,7,8}};
	public static final List<Point[]> recoverMap = new ArrayList<Point []>();

	// 算法参数
	private int stripeSize;
	private int paritySize;

	@Override
	public void init(Codec codec) {
		this.stripeSize = codec.stripeLength;
		this.paritySize = codec.parityLength;
		encodeMap.add(new Point[]{new Point(1, 2), new Point(2, 3), new Point(3, 5)});
		encodeMap.add(new Point[]{new Point(1, 1), new Point(2, 4), new Point(3, 6)});
		encodeMap.add(new Point[]{new Point(1, 1), new Point(1, 4), new Point(2, 1), new Point(3, 7)});
		encodeMap.add(new Point[]{new Point(1, 2), new Point(1, 3), new Point(2, 2), new Point(3, 8)});
		encodeMap.add(new Point[]{new Point(1, 6), new Point(2, 7), new Point(4, 1)});
		encodeMap.add(new Point[]{new Point(1, 5), new Point(2, 8), new Point(4, 2)});
		encodeMap.add(new Point[]{new Point(1, 5), new Point(1, 8), new Point(2, 5), new Point(4, 3)});
		encodeMap.add(new Point[]{new Point(1, 6), new Point(1, 7), new Point(2, 6), new Point(4, 4)});

		recoverMap.add(new Point[]{new Point(1,2),new Point(5,1),new Point(2,3),new Point(3,5)});
		recoverMap.add(new Point[]{new Point(1,4),new Point(5,3),new Point(1,1),new Point(2,1),new Point(3,7)});
		recoverMap.add(new Point[]{new Point(1,6),new Point(5,5),new Point(2,7),new Point(4,1)});
		recoverMap.add(new Point[]{new Point(1,8),new Point(5,7),new Point(1,5),new Point(2,5),new Point(4,3)});
		recoverMap.add(new Point[]{new Point(2,3),new Point(5,1),new Point(1,2),new Point(3,5)});
		recoverMap.add(new Point[]{new Point(2,4),new Point(5,2),new Point(1,1),new Point(3,6)});
		recoverMap.add(new Point[]{new Point(2,7),new Point(5,5),new Point(1,6),new Point(4,1)});
		recoverMap.add(new Point[]{new Point(2,8),new Point(5,6),new Point(1,5),new Point(4,2)});
		recoverMap.add(new Point[]{new Point(3,5),new Point(5,1),new Point(1,2),new Point(2,3)});
		recoverMap.add(new Point[]{new Point(3,6),new Point(5,2),new Point(1,1),new Point(2,4)});
		recoverMap.add(new Point[]{new Point(3,7),new Point(5,3),new Point(1,1),new Point(1,4),new Point(2,1)});
		recoverMap.add(new Point[]{new Point(3,8),new Point(5,4),new Point(1,2),new Point(1,3),new Point(2,2)});
		recoverMap.add(new Point[]{new Point(4,1),new Point(5,5),new Point(1,6),new Point(2,7)});
		recoverMap.add(new Point[]{new Point(4,2),new Point(5,6),new Point(1,5),new Point(2,8)});
		recoverMap.add(new Point[]{new Point(4,3),new Point(5,7),new Point(1,5),new Point(1,8),new Point(2,5)});
		recoverMap.add(new Point[]{new Point(4,4),new Point(5,8),new Point(1,6),new Point(1,7),new Point(2,6)});
	}

	@Override
	public void encode(int[] message, int[] parity) {

	}
	public void encodeSingle(byte[][] inputs,byte[] outputs){
		int rows = inputs.length;
		int cols = inputs[0].length;

		for(int col = 0;col < cols;col++){
			outputs[col] = inputs[0][col];
			for(int row = 1;row < rows;row++){
				outputs[col] ^= inputs[row][col];
			}
		}
	}
	// 修复前4层时不需要借助P2,P2的在data的第二行
	public void decodeDown(byte[][] data,byte[] recovered,int[] erasedLocations){
		//只容忍1个擦除
		int skipIndex = -1;
		if(erasedLocations != null) skipIndex = erasedLocations[0];
		for(int i = 0;i < data[0].length;i++){
			recovered[i] = 0;
			for(int j = 0;j < data.length;j++){
				if(j == skipIndex || j == 1) continue;
				recovered[i] ^= data[j][i];
			}
		}
	}
	public void decodeHalf(byte[][] data,byte[] recovered){
		encodeSingle(data,recovered);
	}
	@Override
	public void encodeBulk(byte[][] inputs, byte[][] outputs) {
	}

	@Override
	public void decode(int[] data, int[] erasedLocations, int[] erasedValues) {

	}

	@Override
	public void decode(int[] data, int[] erasedLocations, int[] erasedValues, int[] locationsToRead, int[] locationsNotToRead) {

	}

	// 与纠删码的n,k性质不同,本编码在修复时必须访问所有其他节点
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
	public int stripeSize() {return 3;}

	@Override
	public int paritySize() {return 2;}

	@Override
	public int symbolSize() {return 0;}
}
