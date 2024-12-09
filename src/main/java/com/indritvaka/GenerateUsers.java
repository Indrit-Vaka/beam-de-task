package com.indritvaka;

import com.indritvaka.options.GenerateUsersOptions;
import com.indritvaka.fn.GenerateUserFn;
import com.indritvaka.fn.ConvertToJsonFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class GenerateUsers {

    private static final Logger logger = LoggerFactory.getLogger(GenerateUsers.class);
    public static void main(String[] args) {

        // Register the pipeline options class
        PipelineOptionsFactory.register(GenerateUsersOptions.class);

        GenerateUsersOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(GenerateUsersOptions.class);

        // Create the pipeline
        Pipeline p = Pipeline.create(options);

        // Generate a list of indices (used to create users)
        List<Integer> userIndices = IntStream.range(0, options.getNumUsers()).boxed().collect(Collectors.toList());
        logger.info("Starting the pipeline with {} users", options.getNumUsers());

        p.apply("CreateIndices", Create.of(userIndices))
                .apply("GenerateUsers", ParDo.of(new GenerateUserFn()))
                .apply("ConvertToJson", ParDo.of(new ConvertToJsonFn()))
                .apply("WriteToGCS", TextIO.write().to(options.getOutputFile()).withoutSharding());
        logger.info("Pipeline execution started");

        p.run().waitUntilFinish();

        logger.info("Pipeline execution finished");

    }
}
