package com.spotify.hdfs2cass;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.spotify.hdfs2cass.cassandra.utils.CassandraParams;
import com.spotify.hdfs2cass.crunch.cql.CQLRecord;
import com.spotify.hdfs2cass.crunch.cql.CQLTarget;
import com.spotify.hdfs2cass.crunch.thrift.ThriftRecord;
import com.spotify.hdfs2cass.crunch.thrift.ThriftTarget;
import org.apache.avro.generic.GenericRecord;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.BasicConfigurator;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemBase;

import java.io.Serializable;
import java.net.URI;
import java.util.List;

/**
 * Crunch job used to import flat Avro files (no maps, lists, etc) into Cassandra Thrift or CQL table.
 * <p>
 * You can specify command line parameters. Default conventions are:
 * - rowkey is ether field by name "rowkey" or first field in data set
 * Other parameters:
 * - timestamp to specify timestamp field name
 * - ttl to specify ttl field name
 * - ignore (can be multiple) to specify fields to ignore
 * </p><p>
 * How to use command line:
 * TODO(zvo): add example usage
 * </p><p>
 * TODO(zvo): add example URIS
 * </p><p>
 * </p>
 */

public class Gcs2Cass extends Configured implements Tool, Serializable {

  @Parameter(names = "--input", required = true)
  protected List<String> input;

  @Parameter(names = "--output", required = true)
  protected String output;

  @Parameter(names = "--rowkey")
  protected String rowkey = "rowkey";

  @Parameter(names = "--timestamp")
  protected String timestamp;

  @Parameter(names = "--ttl")
  protected String ttl;

  @Parameter(names = "--ignore")
  protected List<String> ignore = Lists.newArrayList();

  @Parameter(names = "--bucket")
  protected String gcsBucketURI;

  @Parameter(names = "--project")
  protected String projectId;

  public static void main(String[] args) throws Exception {
    // Logging for local runs. Causes duplicate log lines on actual Hadoop cluster
    BasicConfigurator.configure();
    ToolRunner.run(new Configuration(), new Gcs2Cass(), args);
  }

  protected static GoogleHadoopFileSystem ghfs;

  @Override
  public int run(String[] args) throws Exception {

    new JCommander(this, args);

    // create GHFS for GCS bucket
    ghfs = new GoogleHadoopFileSystem();
    Configuration ghfsConfig = new Configuration();
    ghfsConfig.set(GoogleHadoopFileSystemBase.GCS_PROJECT_ID_KEY, projectId);
    ghfs.initialize(URI.create(gcsBucketURI), ghfsConfig);

    URI outputUri = URI.create(output);

    // Our crunch job is a MapReduce job
    Configuration conf = getConf();
    conf.setBoolean(MRJobConfig.MAP_SPECULATIVE, Boolean.FALSE);
    conf.setBoolean(MRJobConfig.REDUCE_SPECULATIVE, Boolean.FALSE);
    Pipeline pipeline = new MRPipeline(Gcs2Cass.class, conf);

    // Parse & fetch info about target Cassandra cluster
    CassandraParams params = CassandraParams.parse(outputUri);

    PCollection<GenericRecord> records =
        ((PCollection<GenericRecord>)(PCollection) pipeline.read(From.avroFile(inputList(input))));

    String protocol = outputUri.getScheme();
    if (protocol.equalsIgnoreCase("thrift")) {
      records
          // First convert ByteBuffers to ThriftRecords
          .parallelDo(new AvroToThrift(rowkey, timestamp, ttl, ignore), ThriftRecord.PTYPE)
          // Then group the ThriftRecords in preparation for writing them
          .parallelDo(new ThriftRecord.AsPair(), ThriftRecord.AsPair.PTYPE)
          .groupByKey(params.createGroupingOptions())
           // Finally write the ThriftRecords to Cassandra
          .write(new ThriftTarget(outputUri, params));
    }
    else if (protocol.equalsIgnoreCase("cql")) {
      records
          // In case of CQL, convert ByteBuffers to CQLRecords
          .parallelDo(new AvroToCQL(rowkey, timestamp, ttl, ignore), CQLRecord.PTYPE)
          .by(params.getKeyFn(), Avros.bytes())
          .groupByKey(params.createGroupingOptions())
          .write(new CQLTarget(outputUri, params));
    }

    // Execute the pipeline
    PipelineResult result = pipeline.done();
    return result.succeeded() ? 0 : 1;
  }

  private static List<Path> inputList(List<String> inputs) {
    return Lists.newArrayList(Iterables.transform(inputs, new StringToGHFSPath()));
  }

  private static class StringToGHFSPath implements Function<String, Path> {
    @Override
    public Path apply(String resource) {
      return ghfs.getHadoopPath(URI.create(resource));
    }
  }

}
