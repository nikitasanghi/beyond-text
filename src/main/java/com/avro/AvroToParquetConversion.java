package com.avro;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;



import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import parquet.avro.AvroParquetOutputFormat;
import parquet.avro.AvroSchemaConverter;
import parquet.example.data.Group;
import parquet.example.data.simple.SimpleGroupFactory;
import parquet.hadoop.example.ExampleInputFormat;
import parquet.hadoop.example.ExampleOutputFormat;
import parquet.hadoop.metadata.CompressionCodecName;
//import parquet.org.apache.thrift.protocol.TBinaryProtocol.Factory;
import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;








public class AvroToParquetConversion extends Configured implements Tool {
    
    //static String rawschema;
    public static class AvroMapper extends
    Mapper<AvroKey<GenericRecord>, NullWritable, Void, GenericRecord> {
    @Override
    protected void map(AvroKey<GenericRecord> key, NullWritable value,
    Context context) throws IOException, InterruptedException {
    context.write(null, key.datum());
    }
    }
    
    public int run(String[] args) throws Exception {
        Path schemaPath = new Path(args[0]);
        Path inputPath = new Path(args[1]);
        Path outputPath = new Path(args[2]);
        Job job = new Job(getConf());
        job.setJarByClass(getClass());
        Configuration conf = job.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        InputStream in = fs.open(schemaPath);
        Schema avroSchema = new Schema.Parser().parse(in);
        System.out.println(new AvroSchemaConverter().convert(avroSchema).toString());
        FileInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(AvroKeyInputFormat.class);
        job.setOutputFormatClass(AvroParquetOutputFormat.class);
        AvroParquetOutputFormat.setOutputPath(job, outputPath);
        AvroParquetOutputFormat.setSchema(job, avroSchema);
        AvroParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
        AvroParquetOutputFormat.setCompressOutput(job, true);
        /* Impala likes Parquet files to have only a single row group.
        * Setting the block size to a larger value helps ensure this to
        * be the case, at the expense of buffering the output of the
        * entire mapper's split in memory.
        *
        * It would be better to set this based on the files' block size,
        * using fs.getFileStatus or fs.listStatus.
        */
        AvroParquetOutputFormat.setBlockSize(job, 500 * 1024 * 1024);
        job.setMapperClass(AvroMapper.class);
        job.setNumReduceTasks(0);
        return job.waitForCompletion(true) ? 0 : 1;
        }
    

     public static void main(String[] args) throws Exception {
            int exitCode = ToolRunner.run(new AvroToParquetConversion(), args);
            
                System.exit(exitCode);
        }
    }


        


























