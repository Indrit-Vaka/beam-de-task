package com.indritvaka.fn;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class ParseAndPublishFn extends DoFn<String, String> {
    private static final Logger logger = LoggerFactory.getLogger(ParseAndPublishFn.class);

    @ProcessElement
    public void processElement(@Element String jsonLine, OutputReceiver<String> out) {
        try {
            Gson gson = new Gson();
            JsonObject jsonObject = gson.fromJson(jsonLine, JsonObject.class);

            boolean hasRequiredField = jsonObject.has("email") && jsonObject.has("name") && jsonObject.has("phone") && jsonObject.has("address");

            if (hasRequiredField) {
                jsonObject.addProperty("id", UUID.randomUUID().toString());
                out.output(jsonObject.toString());
            } else {
                logger.error("Invalid JSON object: {}", jsonLine);
            }
        } catch (Exception e) {
            logger.error("Failed to parse JSON: {}", jsonLine, e);
        }
    }
}