package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by JackJay on 16/3/30.
 */
public class ComputeBlockHeader extends DataTransferHeader implements Writable{

	private int namespaceId;
	private long blockId;
	private long genStamp;
	private byte blockInStripe;
	private long len;

	public ComputeBlockHeader(VersionAndOpcode versionAndOp) {super(versionAndOp);}

	public ComputeBlockHeader(int dataTransferVersion, byte opCode,int namespaceId,
							  long blockId, long genStamp, byte blockInStripe,long len) {
		super(dataTransferVersion, opCode);
		set(namespaceId,blockId,genStamp,blockInStripe,len);
	}

	public void set(int namespaceId,long blockId, long genStamp, byte blockInStripe,long len){
		this.namespaceId = namespaceId;
		this.blockId = blockId;
		this.genStamp = genStamp;
		this.blockInStripe = blockInStripe;
		this.len = len;
	}

	public int getNamespaceId() {return namespaceId;}

	public long getBlockId() {return blockId;}

	public long getGenStamp() {return genStamp;}

	public byte getBlockInStripe() {return blockInStripe;}

	public long getLen() {return len;}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeLong(namespaceId);
		dataOutput.writeLong(blockId);
		dataOutput.writeLong(genStamp);
		dataOutput.writeByte(blockInStripe);
		dataOutput.writeLong(len);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		namespaceId = dataInput.readInt();
		blockId = dataInput.readLong();
		genStamp = dataInput.readLong();
		blockInStripe = dataInput.readByte();
		len = dataInput.readLong();
	}
}
