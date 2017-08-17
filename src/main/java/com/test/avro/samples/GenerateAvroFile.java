package com.test.avro.samples;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

public class GenerateAvroFile {

	
	public static void main(String[] args) {
		Schema userSchema = null;
		GenericRecord userGenericRecord1 = null;
		GenericRecord userGenericRecord2 = null;
		try {
			userSchema = CreateSchemaUtils.createSchema("users.avsc");
			
			userGenericRecord1 = CreateGenericRecordUtils.createGenericRecordForSchema(userSchema);
			userGenericRecord2 = CreateGenericRecordUtils.createGenericRecordForSchema(userSchema);
			userGenericRecord1.put("name", "Alyssa");
			userGenericRecord1.put("favorite_number", 256);
		
			userGenericRecord2.put("name", "Ben");
			userGenericRecord2.put("favorite_number", 7);
			userGenericRecord2.put("favorite_color", "red");
			
			// Serialize user1 and user2 to disk
			String outputPath = "src/data/resources/"+"users.avro";
			File ouputAvroFile = new File(outputPath);
			DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(userSchema);
			DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
			dataFileWriter.create(userSchema, ouputAvroFile);
			dataFileWriter.append(userGenericRecord1);
			dataFileWriter.append(userGenericRecord2);
			dataFileWriter.close();
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
}
