package com.example.apache_beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;

import java.util.Arrays;
import java.util.List;

public class Filtering {

    public static class FilterThresholdFn extends DoFn<Double, Double>{

        private double threshold = 0;

        public FilterThresholdFn(double threshold) {
            this.threshold = threshold;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            //if(threshold < c.element()) c.output(c.element().toString());

            if(c.element() > threshold){
                c.output(c.element());
            }
        }
    }

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        List<Double> stocks = Arrays.asList(1232.33, 4343.2, 4325.22, 1232.1, 2132.2, 4111.2, 555.2);

        pipeline.apply(Create.of(stocks))
                .apply(ParDo.of(new FilterThresholdFn(1400)));

        pipeline.apply(Create.of(stocks))
                .apply(MapElements.via(new SimpleFunction<Double, Double>() {
                    @Override
                    public Double apply(Double input) {
                        System.out.println("-Pre-filtered: " + input);
                        return input;
                    }
                }))
                .apply(ParDo.of(new FilterThresholdFn(1400)))
                .apply(MapElements.via(new SimpleFunction<Double, Double>() {
                    @Override
                    public Double apply(Double input) {
                        System.out.println("+Post-filtered: " + input);
                        return input;
                    }
                }));

        pipeline.run().waitUntilFinish();
    }

}
