package com.indritvaka.fn;

import com.github.javafaker.Faker;
import com.indritvaka.model.User;
import org.apache.beam.sdk.transforms.DoFn;

public class GenerateUserFn extends DoFn<Integer, User> {
    @ProcessElement
    public void processElement(OutputReceiver<User> receiver) {
        Faker faker = new Faker();
        User user = User.builder()
                .email(faker.internet().emailAddress())
                .name(faker.name().fullName())
                .phone(faker.phoneNumber().cellPhone())
                .address(faker.address().fullAddress())
                .build();
        receiver.output(user);
    }
}
