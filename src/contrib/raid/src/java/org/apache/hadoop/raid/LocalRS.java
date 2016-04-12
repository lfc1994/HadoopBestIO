package org.apache.hadoop.raid;


import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 * Created by JackJay on 16/4/7.
 */
public class LocalRS extends ReedSolomonCode{
	//6,4 locality rs
	private int localStripeSize;
	private int localParitySize;

	@Override
	public void init(Codec codec) {
		this.localStripeSize = codec.stripeLength;
		this.localParitySize = codec.parityLength;
		assert (localStripeSize == 6 && localParitySize == 2);
		super.init(3, 1);
	}

	/**
	 *
	 * @param inputs codec.stripeLength行输入数据,每一行对应一个stripe上的一个block
	 * @param outputs 生成的编码数据
	 */
	@Override
	public void encodeBulk(byte[][] inputs, byte[][] outputs) {
		assert(stripeSize() == 3 && paritySize() == 1);
		//3块为一组,分2组处理
		byte[][] inPiece = new byte[stripeSize()][inputs[0].length];
		byte[][] outPiece = new byte[paritySize()][outputs[0].length];

		for(int group = 0;group < 2;group++){
			for(int i = 0;i < stripeSize();i++){
				System.arraycopy(inputs[i + group * 3], 0, inPiece[i], 0, inputs[0].length);
			}
			super.encodeBulk(inPiece, outPiece);
			// 小心误用引用传递
			System.arraycopy(outPiece[0], 0, outputs[group], 0, outputs[0].length);
		}
	}
	// decodeBulk函数与父类相同
}
