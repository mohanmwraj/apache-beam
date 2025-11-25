package com.example.apache_beam;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ApacheBeamApplication {

	public static void main(String[] args) {

		SpringApplication.run(ApacheBeamApplication.class, args);

        PipelineOptions options = PipelineOptionsFactory.create();

        System.out.println("Runner: " + options.getRunner().getName());
        System.out.println("Job Name: " + options.getJobName());
        System.out.println("OptionsID: " + options.getOptionsId());
        System.out.println("StableUniqueName: "+options.getStableUniqueNames());
        System.out.println("TempLocation: "+options.getTempLocation());
        System.out.println("UserAgent: "+options.getUserAgent());
	}

}
