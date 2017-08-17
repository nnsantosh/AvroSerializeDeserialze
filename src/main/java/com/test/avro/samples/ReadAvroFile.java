package com.test.avro.samples;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

public class ReadAvroFile {

	public static void main(String[] args) {

		Schema userSchema = null;
		DataFileReader<GenericRecord> dataFileReader = null;
		try {
			userSchema = CreateSchemaUtils.createSchema("users.avsc");
			String outputPath = "src/data/resources/" + "users.avro";
			File inputAvroFile = new File(outputPath);
			DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(userSchema);
			 dataFileReader = new DataFileReader<GenericRecord>(inputAvroFile, datumReader);
			GenericRecord user = null;
			while (dataFileReader.hasNext()) {
				// Reuse user object by passing it to next(). This saves us from
				// allocating and garbage collecting many objects for files with
				// many items.
				user = dataFileReader.next(user);
				System.out.println(user);
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			try {
				dataFileReader.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
}
