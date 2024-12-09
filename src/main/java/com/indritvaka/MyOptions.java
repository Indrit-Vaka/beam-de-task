package com.indritvaka;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface MyOptions extends PipelineOptions {
    @Description("Input for the pipeline")
    String getInput();
    void setInput(String value);

    @Description("Output for the pipeline")
    String getOutput();
    void setOutput(String value);
}
