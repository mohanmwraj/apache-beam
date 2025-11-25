package com.example.apache_beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.SimpleFunction;

import java.util.Arrays;
import java.util.List;

public class Aggregation {

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        List<Double> stocks = Arrays.asList(1232.33, 4343.2, 4325.22, 1232.1, 2132.2, 4111.2, 555.2);

        pipeline.apply(Create.of(stocks))
                .apply(Mean.<Double>globally())
                .apply(MapElements.via(new SimpleFunction<Double, Void>() {
                    @Override
                    public Void apply(Double input) {
                        System.out.println("Average Stock Price : " + input);
                        return null;
                    }
                }));

        pipeline.run().waitUntilFinish();

    }
}
