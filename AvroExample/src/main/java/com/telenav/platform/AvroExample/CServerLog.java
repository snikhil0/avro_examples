package com.telenav.platform.AvroExample;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;


public class CServerLog implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -780437026449320064L;
	
	private double lat;
	private double lon;
	private final Schema wrappedSchema;
	
	public CServerLog() throws IOException {
		wrappedSchema = Schema.parse(new File("resources/cserver.avpr"));
	}
	
	public CServerLog(double lat, double lon) throws IOException{
		this.lat = lat;
		this.lon = lon;
		wrappedSchema = Schema.parse(new File("resources/cserver.avpr"));
	}

	// stream as bytebuffer so as to write it out into avro
	public ByteBuffer serialize() throws IOException {
		ByteArrayOutputStream bao = new ByteArrayOutputStream();
		GenericDatumWriter<GenericRecord> w = new GenericDatumWriter<GenericRecord>(wrappedSchema);
		GenericRecord r = new GenericData.Record(wrappedSchema);
		Encoder e = new BinaryEncoder(bao);
		r.put("status", new Utf8("ok"));
		r.put("latitude", lat);
		r.put("longitude", lon);
		w.write(r, e);
		e.flush();
		return ByteBuffer.wrap(bao.toByteArray());
	}
	
	public GenericRecord deserialize(String data) throws IOException {
		DecoderFactory fac = new DecoderFactory();
	    GenericDatumReader<GenericRecord> r = new GenericDatumReader<GenericRecord>(wrappedSchema);
	    Decoder decoder = fac.createBinaryDecoder(data.getBytes(), null);
	    GenericRecord record = r.read(null, decoder);
	    return record;
	}

}
