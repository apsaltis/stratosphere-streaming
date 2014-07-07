package eu.stratosphere.streaming.api.streamrecord;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

import eu.stratosphere.core.io.IOReadableWritable;

public class OutStreamRecord implements IOReadableWritable, Serializable {

	private static final long serialVersionUID = 1L;
	public int partitionHash;
	protected UID uid = new UID();
	private int batchSize;
	private byte[][] tupleBatch;

	public OutStreamRecord() {
	}

	public OutStreamRecord(int batchsize) {
		tupleBatch = new byte[batchsize][];
		this.batchSize = batchsize;
	}

	public OutStreamRecord(OutStreamRecord record, int truncatedSize) {
		tupleBatch = new byte[truncatedSize][];
		this.uid = new UID(Arrays.copyOf(record.getId().getId(), 20));
		for (int i = 0; i < truncatedSize; ++i) {
			this.tupleBatch[i] = record.getBytes(i);
		}
		this.batchSize = tupleBatch.length;
	}

	protected UID getId() {
		return uid;
	}

	public void setBytes(int tupleNumber, byte[] bytes) {

		tupleBatch[tupleNumber] = bytes;

	}

	public byte[] getBytes(int tupleNumber) {

		return tupleBatch[tupleNumber];

	}

	@Override
	public void write(DataOutput out) throws IOException {

		uid.write(out);
		out.writeInt(batchSize);

		for (byte[] bytes : tupleBatch) {
			out.write(bytes);
		}
	}

	@Override
	public void read(DataInput in) throws IOException {
		// TODO Auto-generated method stub

	}

	public void setId(int channelID) {
		uid = new UID(channelID);
	}

}
