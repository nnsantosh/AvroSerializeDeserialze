package com.test.avro.samples;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;

public class CreateSchemaUtils {

	public static Schema createSchema(String avscSchemaFileName) throws IOException {
		String avscFilePath = "src/data/resources/"+avscSchemaFileName;
		Schema schema = new Schema.Parser().parse(new File(avscFilePath));
		return schema;
	}
}
