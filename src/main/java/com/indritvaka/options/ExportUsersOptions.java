package com.indritvaka.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface ExportUsersOptions  extends PipelineOptions {
    @Description("Input for the pipeline")
    @Default.String("gs://indrit-vaka-tmp/beam/user-registration-data/user-data.jsonl")

    String getInput();
    void setInput(String value);

    @Description("Pub/Sub topic to publish messages")
    String getPubSubTopic();
    void setPubSubTopic(String value);
}
