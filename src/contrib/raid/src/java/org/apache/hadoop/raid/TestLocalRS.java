package org.apache.hadoop.raid;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 * Created by JackJay on 16/4/8.
 */
public class TestLocalRS extends TestCase{
	public static final String jsonStr =
			"    [\n" +
					"      {   \n" +
					"        \"id\"            : \"rs\",\n" +
					"        \"parity_dir\"    : \"/raidrs\",\n" +
					"        \"stripe_length\" : 10,\n" +
					"        \"parity_length\" : 4,\n" +
					"        \"priority\"      : 300,\n" +
					"        \"erasure_code\"  : \"org.apache.hadoop.raid.ReedSolomonCode\",\n" +
					"        \"description\"   : \"ReedSolomonCode code\",\n" +
					"        \"simulate_block_fix\"  : true,\n" +
					"      },  \n" +
					"      {   \n" +
					"        \"id\"            : \"xor\",\n" +
					"        \"parity_dir\"    : \"/raid\",\n" +
					"        \"stripe_length\" : 10, \n" +
					"        \"parity_length\" : 1,\n" +
					"        \"priority\"      : 100,\n" +
					"        \"erasure_code\"  : \"org.apache.hadoop.raid.XORCode\",\n" +
					"        \"simulate_block_fix\"  : false,\n" +
					"      },  \n" +
					"      {   \n" +
					"        \"id\"            : \"sr\",\n" +
					"        \"parity_dir\"    : \"/raidsr\",\n" +
					"        \"stripe_length\" : 10, \n" +
					"        \"parity_length\" : 5, \n" +
					"        \"degree\"        : 2,\n" +
					"        \"erasure_code\"  : \"org.apache.hadoop.raid.SimpleRegeneratingCode\",\n" +
					"        \"priority\"      : 200,\n" +
					"        \"description\"   : \"SimpleRegeneratingCode code\",\n" +
					"        \"simulate_block_fix\"  : false,\n" +
					"      },  \n" +
					"      {   \n" +
					"        \"id\"            : \"rs_local\",\n" +
					"        \"parity_dir\"    : \"/raid_rslocal\",\n" +
					"        \"stripe_length\" : 6, \n" +
					"        \"parity_length\" : 2, \n" +
					"        \"erasure_code\"  : \"org.apache.hadoop.raid.LocalRS\",\n" +
					"        \"priority\"      : 50,\n" +
					"        \"description\"   : \"local rs code\",\n" +
					"        \"simulate_block_fix\"  : false,\n" +
					"      },  \n" +
					"    ]\n";

	public void testEncodeBulk(){
		byte [][] in = new byte[6][10];
		byte [][] out = new byte[2][10];

		//赋值
		for(int i = 0;i < 6;i++){
			for(int j = 0;j < 10;j++){
				in[i][j] = (byte)(Math.random()*10);
			}
		}

		LocalRS rs = new LocalRS();

		try{
			Configuration conf = new Configuration();
			conf.set("raid.codecs.json",jsonStr);
			Codec.initializeCodecs(conf);
		}catch(Exception e){
			System.out.print("codec init error");
		}

		Codec codec = Codec.getCodec("rs_local");
		rs.init(codec);

		printArray("in",in);
		rs.encodeBulk(in, out);
		printArray("out",out);
	}

	private void printArray(String name,byte[][] array) {
		System.out.println(name + ":");
		for(int i = 0;i < array.length;i++){
			for(int j = 0;j < array[0].length;j++){
				System.out.print(array[i][j]);
				if(j != array[0].length-1) System.out.print(',');
			}
			System.out.println();
		}
		System.out.println();
	}
}
