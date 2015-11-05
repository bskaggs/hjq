package com.github.bskaggs.hjq;

import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.github.bskaggs.avro_json_hadoop.AvroOrJsonKeyInputFormat;
import com.github.bskaggs.avro_json_hadoop.JsonAsAvroOutputFormat;

public class CLI extends Configured implements Tool{
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		CommandLineParser parser = new DefaultParser();
		
		Options options = new Options();
		options.addOption("m", "mapper", true, "Mapper program");
		options.addOption("r", "reducer", true, "Reducer program");

		options.addOption("i", "input", true, "Comma-separated input files/directories");
		options.addOption("o", "output", true, "Output directory");

		options.addOption("a", "avro-schema", true, "Output schema; makes output files avro");
		
	    CommandLine line = parser.parse(options, args);
	    
	    String mapperProgram = line.getOptionValue("mapper");
		conf.set("hjq.mapper.program", mapperProgram);
		
		String reducerProgram = line.getOptionValue("reducer");
			
		Job job = Job.getInstance(conf, "hjq: " + mapperProgram + (reducerProgram != null ? " -> " + reducerProgram : ""));
		job.setInputFormatClass(AvroOrJsonKeyInputFormat.class);
		TextInputFormat.setInputPaths(job, line.getOptionValue("input"));
		if (reducerProgram != null) {
			job.setMapperClass(HJQKeyValueMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			HJQAbstractReducer.setReducerProgram(job, reducerProgram);
			
			job.setReducerClass(HJQTextReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
		} else {
			job.setMapperClass(HJQKeyMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(NullWritable.class);
			job.setNumReduceTasks(0);
		}
		
		//If there is a schema, output to avro
		String schemaString = line.getOptionValue("avro-schema");
		if (schemaString == null) {
			job.setOutputFormatClass(TextOutputFormat.class);
		} else {
			job.setOutputFormatClass(JsonAsAvroOutputFormat.class);
			AvroJob.setOutputKeySchema(job, new Schema.Parser().parse(schemaString));
		}
		FileOutputFormat.setOutputPath(job, new Path(line.getOptionValue("output")));
		return job.waitForCompletion(true) ? 0 : -1;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new CLI(), args));
	}
	
}
