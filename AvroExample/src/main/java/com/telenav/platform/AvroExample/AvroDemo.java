package com.telenav.platform.AvroExample;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;


/**
 * A demo to show the use of avro 
 * for serialization and deserialization
 * 
 */
public class AvroDemo {
	private final Schema payloadSchema;
	
	public AvroDemo() throws IOException{
		payloadSchema = Schema.parse(new File("resources/log.avpr"));
	}
	
	public void write() throws IOException {
		
		ByteArrayOutputStream bao = new ByteArrayOutputStream();
		GenericDatumWriter<GenericRecord> w = new GenericDatumWriter<GenericRecord>(payloadSchema);
		JsonEncoder e = new JsonEncoder(payloadSchema, bao);
		e.init(new FileOutputStream(new File("resources/test_data.avro")));
		CServerLog log = new CServerLog("ok",37.371249,-122.064972);
		
		GenericRecord r = new GenericData.Record(payloadSchema);
		r.put("timestamp", 1000001L);
		r.put("appid", 1);
		r.put("body", log.serialize());
		w.write(r, e);
		e.flush();
		
		bao.close();
	}

	public void read() throws IOException {
		Schema s = Schema.parse(new File("resources/log.avpr"));
	    GenericDatumReader<GenericRecord> r = new GenericDatumReader<GenericRecord>(s);
	    Decoder decoder = new JsonDecoder(s, new FileInputStream(new File("resources/test_data.avro")));
	    GenericRecord rec = (GenericRecord)r.read(null, decoder);
	    
	    CServerLog cserver = new CServerLog();
	    ByteBuffer bb = (ByteBuffer)rec.get("body");
		GenericRecord log =  cserver.deserialize(bb.toString());
    	System.out.println("timestamp: " + rec.get("timestamp") + "; appid: " 
    	+ rec.get("appid"));
    	System.out.println("body: [status: " + log.get("status") + "; latitude: " 
    	+ log.get("latitude") + "; longitude: " + log.get("longitude"));
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
