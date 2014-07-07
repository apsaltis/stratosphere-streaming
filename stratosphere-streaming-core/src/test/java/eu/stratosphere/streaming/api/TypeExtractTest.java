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

package eu.stratosphere.streaming.api;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;

import org.junit.Test;

import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;
import eu.stratosphere.api.java.typeutils.TypeExtractor;
import eu.stratosphere.api.java.typeutils.runtime.TupleSerializer;
import eu.stratosphere.pact.runtime.plugable.DeserializationDelegate;
import eu.stratosphere.pact.runtime.plugable.SerializationDelegate;
import eu.stratosphere.streaming.api.streamrecord.ArrayStreamRecord;
import eu.stratosphere.streaming.api.streamrecord.OutStreamRecord;
import eu.stratosphere.types.TypeInformation;

public class TypeExtractTest {

	public static class MySuperlass<T> implements Serializable {
		private static final long serialVersionUID = 1L;

	}

	public static class Myclass extends MySuperlass<Integer> {

		private static final long serialVersionUID = 1L;

	}

	@Test
	public void test() throws IOException, ClassNotFoundException {

		Myclass f = new Myclass();

		TypeInformation<?> ts = TypeExtractor.createTypeInfo(MySuperlass.class, f.getClass(), 0,
				null, null);

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos;

		oos = new ObjectOutputStream(baos);
		oos.writeObject(f);

		ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));

		assertTrue(true);
		
		TupleTypeInfo<Tuple> tti = ((TupleTypeInfo) (TypeExtractor
				.getForObject(new Tuple1<Integer>(2))));
		
		TupleSerializer<Tuple> tss = tti.createSerializer();

		SerializationDelegate<Tuple> serializationDelegate = new SerializationDelegate<Tuple>(tss);
		System.out.println(serializationDelegate);

		ByteArrayOutputStream simpleOutputStream = new ByteArrayOutputStream();
		ObjectOutputStream o;

		
		o = new ObjectOutputStream(simpleOutputStream);
		Tuple1<Integer> tuple = new Tuple1<Integer>(1000000000);

		serializationDelegate.setInstance(tuple);
		serializationDelegate.write(o);
		o.flush();

		ByteArrayOutputStream b2 = new ByteArrayOutputStream();
		ObjectOutputStream o2= new ObjectOutputStream(b2);
		
		
		o2.write(Arrays.copyOfRange(simpleOutputStream.toByteArray(), 6, simpleOutputStream.toByteArray().length));
		o2.flush();
		System.out.println(Arrays.toString(b2.toByteArray()));
		System.out.println(Arrays.toString(simpleOutputStream.toByteArray()));

		System.out.println(tss);

		ArrayStreamRecord as = new ArrayStreamRecord();
		
		DeserializationDelegate<Tuple> dd = new DeserializationDelegate<Tuple>(tss);
		as.setDeseralizationDelegate(dd, tss);

		

		ObjectInputStream i = new ObjectInputStream(new ByteArrayInputStream(simpleOutputStream.toByteArray()));

		dd.setInstance(tss.createInstance());
		dd.read(i);
		System.out.println(dd.getInstance());
		
		ObjectInputStream i2 = new ObjectInputStream(new ByteArrayInputStream(b2.toByteArray()));
		//as.read(i2);
		dd.setInstance(tss.createInstance());
		dd.read(i2);
		System.out.println(dd.getInstance());
	}

}
