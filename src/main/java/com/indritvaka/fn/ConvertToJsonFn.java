package com.indritvaka.fn;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.indritvaka.model.User;
import org.apache.beam.sdk.transforms.DoFn;

public class ConvertToJsonFn extends DoFn<User, String> {
    @ProcessElement
    public void processElement(@Element User element, OutputReceiver<String> receiver) {
        Gson gson = new GsonBuilder().create();
        receiver.output(gson.toJson(element));
    }
}
