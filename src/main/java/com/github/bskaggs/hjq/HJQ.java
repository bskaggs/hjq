package com.github.bskaggs.hjq;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
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
import com.github.bskaggs.jjq.JJQ;
import com.github.bskaggs.jjq.JJQCompilationException;

/**
 * Command-line runner for HJQ jobs.
 * 
 * @author bskaggs
 *
 */
public class HJQ extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf);
		
		CommandLineParser parser = new GnuParser();

		Options options = new Options();
		
		options.addOption("m", "mapper", true, "Mapper program (default is '.')");
		options.addOption("c", "combiner", true, "Optional combiner program");
		options.addOption("r", "reducer", true, "Optional reducer program");
		options.addOption("n", "num-reducers", true, "Number of reducers (default: " + job.getNumReduceTasks() + ")");

		options.addOption("i", "input", true, "Comma-separated input files/directories");
		options.addOption("o", "output", true, "Output directory");

		options.addOption("a", "avro-schema", true, "Output schema string; makes output files in avro format");
		options.addOption(null, "avro-schema-file", true, "Output schema file; makes output files in avro format");
		
		options.addOption("j", "job-name", true, "Name for Hadoop job.  (default is derived from mapper/reducer program strings)");
		options.addOption("h", "help", false, "Print this help message");

		CommandLine line = parser.parse(options, args);
		if (line.hasOption("help")) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("hjq", "hjq makes it easy to run Hadoop Map/Reduce jobs using jq syntax on JSON/Avro files.", 
					options,
					"For more information, visit <https://github.com/bskaggs/hjq>. ");
			
			return -1;
		}
		
		String mapperProgram = line.getOptionValue("mapper");
		if (mapperProgram != null) {
			try {
				new JJQ(mapperProgram, null);
			} catch (JJQCompilationException ex) {
				System.err.println("Error in mapper program: " + mapperProgram);
				return -2;
			}
		}
		
		String combinerProgram = line.getOptionValue("combiner");
		if (combinerProgram != null) {
			try {
				new JJQ(combinerProgram, null);
			} catch (JJQCompilationException ex) {
				System.err.println("Error in combiner program: " + combinerProgram);
				return -3;
			}
		}
		
		String reducerProgram = line.getOptionValue("reducer");
		if (reducerProgram != null) {
			try {
				new JJQ(reducerProgram, null);
			} catch (JJQCompilationException ex) {
				System.err.println("Error in reducer program: " + reducerProgram);
				return -4;
			}
		}
		
		String jobName = line.getOptionValue("job-name");
		if (jobName == null) {
			jobName = "hjq: " + mapperProgram + (reducerProgram != null ? " -> " + reducerProgram : "");
		}
		job.setJobName(jobName);
				
		job.setInputFormatClass(AvroOrJsonKeyInputFormat.class);
		
		HJQAbstractMapper.setMapperProgram(job, mapperProgram);

		TextInputFormat.setInputPaths(job, line.getOptionValue("input"));
		if (reducerProgram != null) {
			job.setMapperClass(HJQKeyValueMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			if (combinerProgram != null) {
				job.setCombinerClass(HJQTextCombiner.class);
				HJQTextCombiner.setCombinerProgram(job, combinerProgram);
			}
			
			job.setReducerClass(HJQTextReducer.class);
			HJQTextReducer.setReducerProgram(job, reducerProgram);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			
			String numReducersString = line.getOptionValue("num-reducers");
			if (numReducersString != null) {
				int numReducers = 0;
				try {
					numReducers = Integer.parseInt(numReducersString);
				} catch (NumberFormatException e) {
					numReducers = -1;
				}
				if (numReducers < 0) {
					System.err.println("Invalid value for num-reducers, should be non-negative integer.");
					return(2);
				}
				job.setNumReduceTasks(numReducers);
			}
		} else {
			job.setMapperClass(HJQKeyMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(NullWritable.class);
			job.setNumReduceTasks(0);
		}

		String schemaString = line.getOptionValue("avro-schema");
		String schemaPathString = line.getOptionValue("avro-schema-file");
		
		if (schemaString != null && schemaPathString != null) {
			System.err.println("You must specify at most one of avro-schema or avro-schema-string, not both.");
			return(1);
		}
		// If there is a schema file and no schema string, load the file
		if (schemaString == null && schemaPathString != null) {
			Path schemaPath = new Path(schemaPathString);
			FSDataInputStream in = schemaPath.getFileSystem(conf).open(schemaPath);
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			IOUtils.copyBytes(in, out, conf);
			schemaString = StandardCharsets.UTF_8.decode(ByteBuffer.wrap(out.toByteArray())).toString();
		}

		// If there is no schema, output to Text, otherwise output to Avro
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
		System.exit(ToolRunner.run(new HJQ(), args));
	}
}
