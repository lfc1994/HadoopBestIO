package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.hdfs.protocol.Block;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by JackJay on 16/3/28.
 */
public class ComputeBlockSender {

	private FSDatasetInterface reader;
	private InputStream blockIn = null;
	private DataOutputStream out = null;
	private Block block;
	private int namespaceId;
	private byte blockInStripe;
	
	public ComputeBlockSender(int namespaceId,Block block,byte blockInStripe,
							  DataNode datanode,DataOutputStream out) {
		this.reader = datanode.data;
		this.block = block;
		this.namespaceId = namespaceId;
		this.blockInStripe = blockInStripe;
		this.out = out;
	}

	public void computeAndSend(){
		//blockIn = reader.getBlockInputStream(namespaceId,block,offset1);
	}

	private void sendGroupPair(){

	}

}
