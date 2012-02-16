package com.telenav.platform.AvroExample;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.util.Utf8;


/**
 * A demo to show the use of avro 
 * for serialization and deserialization
 * 
 */
public class AvroDemo {
	private final Schema payloadSchema;
	private final Schema bodySchema;
	
	public AvroDemo() throws IOException{
		payloadSchema = Schema.parse(new File("resources/log.avpr"));
		bodySchema = Schema.parse(new File("resources/cserver.avpr"));
	}
	
	public void write() throws IOException {
		ByteArrayOutputStream bao = new ByteArrayOutputStream();
		GenericDatumWriter<GenericRecord> w = new GenericDatumWriter<GenericRecord>(payloadSchema);
		JsonEncoder e = new JsonEncoder(payloadSchema, bao);
		e.init(new FileOutputStream(new File("resources/test_data.avro")));
		
		GenericRecord inner_record = new GenericData.Record(bodySchema);
		inner_record.put("status", new Utf8("ok"));
		inner_record.put("latitude", 37.371249);
		inner_record.put("longitude", -122.064972);
		
		ByteArrayOutputStream inner_bao = new ByteArrayOutputStream();
		BinaryEncoder be = new BinaryEncoder(inner_bao);
		GenericDatumWriter<GenericRecord> inner_writer = new GenericDatumWriter<GenericRecord>(bodySchema);
		inner_writer.write(inner_record, be);
		be.flush();
		
		GenericRecord r = new GenericData.Record(payloadSchema);
		r.put("timestamp", 1000001L);
		r.put("appid", 1);
		r.put("body", ByteBuffer.wrap(inner_bao.toByteArray()));
		w.write(r, e);
		e.flush();
		
		bao.close();
	}

	public void read() throws IOException {
		Schema s = Schema.parse(new File("resources/log.avpr"));
	    GenericDatumReader<GenericRecord> r = new GenericDatumReader<GenericRecord>(s);
	    Decoder decoder = new JsonDecoder(s, new FileInputStream(new File("resources/test_data.avro")));
	    GenericRecord rec = (GenericRecord)r.read(null, decoder);
	    
	    ByteBuffer bb = (ByteBuffer)rec.get("body");
	    
	    Schema inner_schema = Schema.parse(new File("resources/cserver.avpr"));
	    GenericDatumReader<GenericRecord> inner_reader = new GenericDatumReader<GenericRecord>(inner_schema);
	    DecoderFactory fac = new DecoderFactory();
	    Decoder inner_decoder = fac.createBinaryDecoder(bb.array(), null);
	    
	    GenericRecord inner_rec = (GenericRecord)inner_reader.read(null, inner_decoder);
	    
    	System.out.println("timestamp: " + rec.get("timestamp") + "; appid: " 
    	+ rec.get("appid"));
    	System.out.println("body: [status: " + inner_rec.get("status") + "; latitude: " 
    	+ inner_rec.get("latitude") + "; longitude: " + inner_rec.get("longitude"));
	}
	
	public static void main(String[] args) {
		System.out.println("Hello World!");
		try {
			AvroDemo demo = new  AvroDemo();
			demo.write();
			demo.read();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
