package com.indritvaka.options;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;

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
