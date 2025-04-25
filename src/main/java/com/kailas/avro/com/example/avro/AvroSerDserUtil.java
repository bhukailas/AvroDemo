package com.kailas.avro.com.example.avro;

import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

@Component
public class AvroSerDserUtil {

    public <T extends SpecificRecord> T jsonToAvro(String json, Class<T> clazz) throws IOException, IOException, InstantiationException, IllegalAccessException {
        SpecificDatumReader<T> reader = new SpecificDatumReader<>(clazz);
        Decoder decoder = DecoderFactory.get().jsonDecoder(clazz.newInstance().getSchema(), json);
        return reader.read(null, decoder);
    }

    public String avroToJson(SpecificRecord record) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        SpecificDatumWriter<SpecificRecord> writer = new SpecificDatumWriter<>(record.getSchema());
        Encoder encoder = EncoderFactory.get().jsonEncoder(record.getSchema(), outputStream);
        writer.write(record, encoder);
        encoder.flush();
        return outputStream.toString();
    }
}
