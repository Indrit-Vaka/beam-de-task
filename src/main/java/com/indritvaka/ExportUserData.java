package com.indritvaka;

import com.indritvaka.fn.ParseAndPublishFn;
import com.indritvaka.options.ExportUsersOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExportUserData {

    private static final Logger logger = LoggerFactory.getLogger(ExportUserData.class);

    public static void main(String[] args) {
        PipelineOptionsFactory.register(ExportUsersOptions.class);

        ExportUsersOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(ExportUsersOptions.class);
        logger.info("Starting the pipeline with options {}", options);

        Pipeline p = Pipeline.create(options);


        p.apply("Read Users", TextIO.read().from(options.getInput()))
                .apply("Windowing", Window.into(FixedWindows.of(Duration.standardSeconds(10))))
                .apply("Parse and Publish to Pub/Sub", ParDo.of(new ParseAndPublishFn()))
                .apply("Publish to Pub/Sub", PubsubIO.writeStrings().to(options.getPubSubTopic()));
        logger.info("Pipeline execution started");

        p.run().waitUntilFinish();
        logger.info("Pipeline execution finished");

    }

}
