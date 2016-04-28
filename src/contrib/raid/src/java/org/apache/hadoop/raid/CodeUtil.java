package org.apache.hadoop.raid;

/**
 * Created by JackJay on 16/4/27.
 */
public class CodeUtil {
	public static Codec createCodec(String id,String codeClass,int stripeLen,int parityLen){
		return new Codec(id,stripeLen,parityLen,
				codeClass,"/raid_"+id,100,
				"descp","/tmp/raid_"+id,"/tmp/raid_"+id+"_har",
				false,false);
	}
}
