package com.indritvaka;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;
import java.util.UUID;

public class ExportUserData {

    public static void main(String[] args) {
        PipelineOptionsFactory.register(ExportUsersOptions.class);
        ExportUsersOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(ExportUsersOptions.class);

        Pipeline p = Pipeline.create(options);


        p.apply("Read Users", TextIO.read().from(options.getInput()))
                .apply("Windowing", Window.into(FixedWindows.of(Duration.standardSeconds(10))))
                .apply("Parse and Publish to Pub/Sub", ParDo.of(new ParseAndPublishFn()))
                .apply("Publish to Pub/Sub", PubsubIO.writeStrings().to(options.getPubSubTopic()));


        p.run().waitUntilFinish();

    }

    // DoFn to parse JSON and validate records
    static class ParseAndPublishFn extends DoFn<String, String> {

        @ProcessElement
        public void processElement(@Element String jsonLine, OutputReceiver<String> out) {
            try {
                Gson  gson = new Gson();
                // Parse JSON
                JsonObject jsonObject = gson.fromJson(jsonLine, JsonObject.class);

                // Validate required fields
                if (jsonObject.has("email") && jsonObject.has("name") && jsonObject.has("phone") && jsonObject.has("address")) {
                    // Emit valid JSON lines to Pub/Sub
                    jsonObject.addProperty("id", UUID.randomUUID().toString());

                    out.output(jsonObject.toString());
                } else {
                    System.err.println("Invalid JSON object: " + jsonLine);
                }
            } catch (Exception e) {
                System.err.println("Failed to parse JSON: " + jsonLine);
                e.printStackTrace();
            }
        }
    }
}
