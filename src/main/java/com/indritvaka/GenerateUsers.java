package com.indritvaka;

import com.github.javafaker.Faker;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.indritvaka.model.User;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class GenerateUsers {


    public interface GenerateUsersOptions extends PipelineOptions {
        @Description("Number of users to generate")
        @Default.Integer(1000)
        int getNumUsers();

        void setNumUsers(int value);

        @Description("Path to the output JSON file on GCS")
        @Default.String("gs://indrit-vaka-tmp/beam/user-registration-data/user-data.jsonl")
        String getOutputFile();

        void setOutputFile(String value);
    }

    public static void main(String[] args) {

        PipelineOptionsFactory.register(GenerateUsersOptions.class);
        GenerateUsersOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(GenerateUsersOptions.class);

        Pipeline p = Pipeline.create(options);

        List<Integer> userIndices = IntStream.range(0, options.getNumUsers()).boxed().collect(Collectors.toList());


        p.apply("CreateIndices", Create.of(userIndices))
                .apply("GenerateUsers", ParDo.of(new GenerateUserFn()))
                .apply("Convert to JSON", ParDo.of(new ConvertToJsonFn()))
                .apply("Write to GCS", TextIO.write().to(options.getOutputFile()).withoutSharding());

        p.run().waitUntilFinish();

    }
    static class ConvertToJsonFn extends DoFn<User, String> {
        @ProcessElement
        public void processElement(@Element User element, OutputReceiver<String> receiver) {
            Gson gson = new GsonBuilder().create();
            receiver.output(gson.toJson(element));
        }
    }
    static class GenerateUserFn extends DoFn<Integer, User> {
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
}
