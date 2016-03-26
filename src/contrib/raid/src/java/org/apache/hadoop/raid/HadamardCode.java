package org.apache.hadoop.raid;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by JackJay on 16/3/2.
 */
public class HadamardCode{

	public static final Log LOG = LogFactory.getLog(HadamardCode.class);

	public static int DIMENSION = 32;
	public static final int[][] COEFFICIENT = {{4, 8, 10, 7}, {8, 4, 7, 10}, {5, 8, 10, 7}, {9, 4, 7, 10}
			, {4, 9, 10, 7}, {8, 5, 7, 10}, {5, 9, 10, 7}, {9, 5, 7, 10}
			, {4, 8, 6, 7}, {8, 4, 3, 10}, {5, 8, 6, 7}, {9, 4, 3, 10}
			, {4, 9, 6, 7}, {8, 5, 3, 10}, {5, 9, 6, 7}, {9, 5, 3, 10}
			, {4, 8, 10, 3}, {8, 4, 7, 6}, {5, 8, 10, 3}, {9, 4, 7, 6}
			, {4, 9, 10, 3}, {8, 5, 7, 6}, {5, 9, 10, 3}, {9, 5, 7, 6}
			, {4, 8, 6, 3}, {8, 4, 3, 6}, {5, 8, 6, 3}, {9, 4, 3, 6}
			, {4, 9, 6, 3}, {8, 5, 3, 6}, {5, 9, 6, 3}, {9, 5, 3, 6}};

	private int stripeSize;
	private int paritySize;

	//每一个对象都拥有的记录层数的变量
	private int current_dim;
	private int[] dataBuff;

	public void init(Codec codec) {
		this.stripeSize = codec.stripeLength;
		this.paritySize = codec.parityLength;
		this.dataBuff = new int[paritySize + stripeSize];
		LOG.info("Initialized " + HadamardCode.class +
				" stripeLength:" + codec.stripeLength +
				" parityLength:" + codec.parityLength);
		current_dim = 0;
	}

	public void setCurrent_dim(int current_dim) {
		this.current_dim = current_dim;
	}
	public void setCurrent_dim(long current_dim) {
		this.current_dim = (int)current_dim;
	}

	public void encode(int[] message, int[] parity) {
		assert (message.length == stripeSize && parity.length == paritySize);
		for (int i = 0; i < message.length; i++) {
			parity[0] += message[i];
			parity[1] += COEFFICIENT[current_dim][i] * message[i];
		}
		parity[0] %= 11;
		parity[1] %= 11;
	}

	public void encodeBulk(byte[][] inputs, byte[][] outputs) {
		assert (stripeSize == inputs.length);
		assert (paritySize == outputs.length);

		int[] data = new int[stripeSize];
		int[] code = new int[paritySize];

		for (int j = 0; j < outputs[0].length; j++) {
			for (int i = 0; i < paritySize; i++) {
				code[i] = 0;
			}
			for (int i = 0; i < stripeSize; i++) {
				data[i] = inputs[i][j] & 0x000000FF;
			}
			encode(data, code);
			for (int i = 0; i < paritySize; i++) {
				outputs[i][j] = (byte) code[i];
			}
		}
	}
}
