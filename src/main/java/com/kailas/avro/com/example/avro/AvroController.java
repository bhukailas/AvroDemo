package com.kailas.avro.com.example.avro;

import com.example.avro.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/avro")
public class AvroController {

    @Autowired
    private AvroService avroService;

    @GetMapping("/serialize")
    public String serialize() {
        List<User> users = new ArrayList<>();
        users.add(User.newBuilder().setId("id1").setName("1").setEmail("john@example.com").build());
        users.add(User.newBuilder().setId("id2").setName("Jane Smith").setEmail("jane@example.com").build());
        users.add(User.newBuilder().setId("id1").setName("1").setEmail("john@example.com").build());
        users.add(User.newBuilder().setId("id2").setName("Jane Smith").setEmail("jane@example.com").build());
        users.add(User.newBuilder().setId("id1").setName("1").setEmail("john@example.com").build());
        users.add(User.newBuilder().setId("id2").setName("Jane Smith").setEmail("jane@example.com").build());
        users.add(User.newBuilder().setId("id1").setName("1").setEmail("john@example.com").build());
        users.add(User.newBuilder().setId("id2").setName("Jane Smith").setEmail("jane@example.com").build());
        users.add(User.newBuilder().setId("id1").setName("1").setEmail("john@example.com").build());
        users.add(User.newBuilder().setId("id2").setName("Jane Smith").setEmail("jane@example.com").build());
        users.add(User.newBuilder().setId("id1").setName("1").setEmail("john@example.com").build());
        users.add(User.newBuilder().setId("id2").setName("Jane Smith").setEmail("jane@example.com").build());
        users.add(User.newBuilder().setId("id1").setName("1").setEmail("john@example.com").build());
        users.add(User.newBuilder().setId("id2").setName("Jane Smith").setEmail("jane@example.com").build());
        users.add(User.newBuilder().setId("id1").setName("1").setEmail("john@example.com").build());
        users.add(User.newBuilder().setId("id2").setName("Jane Smith").setEmail("jane@example.com").build());
        users.add(User.newBuilder().setId("id1").setName("1").setEmail("john@example.com").build());
        users.add(User.newBuilder().setId("id2").setName("Jane Smith").setEmail("jane@example.com").build());
        users.add(User.newBuilder().setId("id1").setName("1").setEmail("john@example.com").build());
        users.add(User.newBuilder().setId("id2").setName("Jane Smith").setEmail("jane@example.com").build());

        avroService.serializeToFile(users);
        return "Data serialized to users.avro";
    }

    @GetMapping("/deserialize")
    public List<UserDto> deserialize() {
        List<User> avroUsers = avroService.deserializeFromFile();
        return avroUsers.stream().map(user -> new UserDto(user.getId().toString(), user.getName().toString(), user.getEmail().toString()))
                .collect(Collectors.toList());
    }

    @PostMapping("/se_des")
    public void seDes() throws IOException, InstantiationException, IllegalAccessException {
        avroService.seDesAvroObject();
    }
}
