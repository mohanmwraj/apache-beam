package com.example.apache_beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;

import java.util.Objects;

public class TotalScoreComputation {

    private static final String CSV_HEADER =
            "ID,Name,Physics,Chemistry,Math,English,Biology,History";

    private static class FilterHeaderFn extends DoFn<String, String> {

        private final String header;

        private FilterHeaderFn(String header) {
            this.header = header;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            String row = c.element();

            if(!row.isEmpty() && !row.equals(header)) {
                c.output(row);
            }
        }
    }

    private static class ComputeTotalScoreFn extends DoFn<String, KV<String, Integer>> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] data = Objects.requireNonNull(c.element()).split(",");
            //String id = data[0].trim();
            String name = data[1].trim();

            Integer totalScore = Integer.parseInt(data[2].trim()) +
                    Integer.parseInt(data[3].trim()) +
                    Integer.parseInt(data[4].trim()) +
                    Integer.parseInt(data[5].trim()) +
                    Integer.parseInt(data[6].trim()) +
                    Integer.parseInt(data[7].trim());

            c.output(KV.of(name, totalScore));
        }
    }

    private static class ConvertToStringFn extends DoFn<KV<String, Integer> , String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(Objects.requireNonNull(c.element()).getKey() +
                    "," + Objects.requireNonNull(c.element()).getValue());
        }
    }

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(TextIO.read().from("src/main/resources/source/studentScore.csv"))
                .apply(ParDo.of(new FilterHeaderFn(CSV_HEADER)))
                .apply(ParDo.of(new ComputeTotalScoreFn()))
                .apply(ParDo.of(new ConvertToStringFn()))
                .apply(TextIO.write().to("src/main/resources/sink/student_total_scores.csv") // this will give no of files based on shards
                        .withHeader("Name,Total")
                        .withNumShards(1)); // To get result in single shard - but may impact parallelism

        pipeline.run().waitUntilFinish();
    }
}
