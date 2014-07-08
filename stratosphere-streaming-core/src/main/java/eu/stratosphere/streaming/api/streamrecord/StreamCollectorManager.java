/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.streaming.api.streamrecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.pact.runtime.plugable.SerializationDelegate;
import eu.stratosphere.runtime.io.api.RecordWriter;
import eu.stratosphere.util.Collector;

public class StreamCollectorManager<T extends Tuple> implements Collector<T> {

	ArrayList<StreamCollector> notPartitionedCollectors;
	ArrayList<StreamCollector[]> partitionedCollectors;
	List<RecordWriter<OutStreamRecord>> partitionedOutputs;
	List<RecordWriter<OutStreamRecord>> notPartitionedOutputs;
	SerializationDelegate<Tuple> serializationDelegate;
	ByteArrayOutputStream bos;
	int keyPostition;
	byte[] bytes;
	ObjectOutputStream out;

	// TODO consider channelID
	public StreamCollectorManager(List<Integer> batchSizesOfNotPartitioned,
			List<Integer> batchSizesOfPartitioned, List<Integer> parallelismOfOutput,
			int keyPosition, long batchTimeout, int channelID,
			SerializationDelegate<Tuple> serializationDelegate,
			List<RecordWriter<OutStreamRecord>> partitionedOutputs,
			List<RecordWriter<OutStreamRecord>> notPartitionedOutputs) {

		bos = new ByteArrayOutputStream();
		try {
			out = new ObjectOutputStream(bos);
		} catch (IOException e) {
			e.printStackTrace();
		}
		notPartitionedCollectors = new ArrayList<StreamCollector>(batchSizesOfNotPartitioned.size());
		partitionedCollectors = new ArrayList<StreamCollector[]>(batchSizesOfPartitioned.size());

		this.keyPostition = keyPosition;

		for (int i = 0; i < batchSizesOfNotPartitioned.size(); i++) {
			notPartitionedCollectors.add(new StreamCollector(batchSizesOfNotPartitioned.get(i),
					batchTimeout, channelID, notPartitionedOutputs.get(i)));
		}

		for (int i = 0; i < batchSizesOfPartitioned.size(); i++) {
			@SuppressWarnings("unchecked")
			StreamCollector[] collectors = new StreamCollector[parallelismOfOutput.get(i)];
			for (int j = 0; j < collectors.length; j++) {
				collectors[j] = new StreamCollector(batchSizesOfPartitioned.get(i), batchTimeout,
						channelID, partitionedOutputs.get(i));
				collectors[j].streamRecord.partitionHash = j;
			}
			partitionedCollectors.add(collectors);
		}

		this.serializationDelegate = serializationDelegate;
	}

	// TODO copy here instead of copying inside every StreamCollector
	@Override
	public void collect(T tuple) {
		bos.reset();
		try {
			out = new ObjectOutputStream(bos);
		} catch (IOException e) {
			e.printStackTrace();
		}
		int partitionHash = Math.abs(tuple.getField(keyPostition).hashCode());
		try {
			serializationDelegate.setInstance(tuple);
			serializationDelegate.write(out);
			out.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
		//Cut off serialization of the byte array itself
		byte[] bytes = bos.toByteArray();
		collect(Arrays.copyOfRange(bytes, 6, bytes.length), partitionHash);

	}

	private void collect(byte[] bytes, int partitionHash) {
		for (StreamCollector collector : notPartitionedCollectors) {
			collector.collect(bytes);
		}

		for (StreamCollector[] collectors : partitionedCollectors) {
			collectors[partitionHash % collectors.length].collect(bytes);
		}
	}

	@Override
	public void close() {

	}
}
