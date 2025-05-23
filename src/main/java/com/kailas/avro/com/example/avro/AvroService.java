package com.kailas.avro.com.example.avro;

import com.example.avro.User;
import lombok.RequiredArgsConstructor;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
public class AvroService {

    private static final String FILE_PATH = "users.avro";
    private final UserProducerService userProducerService;
    private final AvroSerDserUtil util;

    public void serializeToFile(List<User> users) {
        DatumWriter<User> userDatumWriter = new SpecificDatumWriter<>(User.class);
        try (DataFileWriter<User> dataFileWriter = new DataFileWriter<>(userDatumWriter)) {
            dataFileWriter.create(users.get(0).getSchema(), new File(FILE_PATH));
            for (User user : users) {
                dataFileWriter.append(user);
                userProducerService.sendEmployee(user);
            }
            System.out.println("Data successfully serialized to file: " + FILE_PATH);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void saveEmployee(User user) {

    }


    public List<User> deserializeFromFile() {
        List<User> users = new ArrayList<>();
        DatumReader<User> userDatumReader = new SpecificDatumReader<>(User.class);
        try (DataFileReader<User> dataFileReader = new DataFileReader<>(new File(FILE_PATH), userDatumReader)) {
            while (dataFileReader.hasNext()) {
                users.add(dataFileReader.next());
            }
            System.out.println("Data successfully deserialized from file: " + FILE_PATH);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return users;
    }

    public void seDesAvroObject() throws IOException, InstantiationException, IllegalAccessException {
        User user = User.newBuilder()
                .setEmail("abc@def.com")
                .setId("1")
                .setName("kailas")
                .build();

        String userJsonString = util.avroToJson(user);
        System.out.println(userJsonString);

        User desUser = util.jsonToAvro(userJsonString, User.class);
        System.out.println(desUser.getId()+" - "+desUser.getEmail() + " - "+desUser.getEmail());
    }
}
