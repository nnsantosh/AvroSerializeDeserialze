package com.test.avro.samples;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public class CreateGenericRecordUtils{
	
	public static GenericRecord createGenericRecordForSchema(Schema schema) {
		GenericRecord genericRecord = new GenericData.Record(schema);
		return genericRecord;
		
	}

}
