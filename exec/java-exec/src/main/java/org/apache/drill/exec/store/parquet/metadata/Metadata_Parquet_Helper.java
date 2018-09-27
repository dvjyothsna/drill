package org.apache.drill.exec.store.parquet.metadata;

import com.google.gson.Gson;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.drill.shaded.guava.com.google.common.base.Stopwatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;



public class Metadata_Parquet_Helper {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Metadata_Parquet_Helper.class);

  public static Configuration conf = new Configuration();

  public static ParquetWriter<Group> initWriter(MessageType schema, Path path, boolean dictEncoding) throws IOException {

    GroupWriteSupport.setSchema(schema, conf);

    ParquetWriter<Group> writer =
            new ParquetWriter<Group>(path,
                    ParquetFileWriter.Mode.OVERWRITE,
                    new GroupWriteSupport(),
                    CompressionCodecName.SNAPPY,
                    1024,
                    1024,
                    512,
                    dictEncoding, // enable dictionary encoding,
                    false,
                    ParquetProperties.WriterVersion.PARQUET_1_0, conf
            );

    return writer;
  }
  public static String createMetadataSchema(Metadata_V3.ParquetTableMetadata_v3 parquetTableMetadata) {
    String schema = null;
    if (parquetTableMetadata  != null && parquetTableMetadata.getFiles().size() != 0) {
      schema = "message metadata { \n";
      schema = schema + "required INT32 fid; \n";
      schema = schema + "required binary path; \n";
      schema = schema + "required INT64 length; \n";
      schema = schema + "required INT64 start; \n";
      schema = schema + "required INT64 rgLength; \n";
      schema = schema + "required INT64 rowCount; \n";
      schema = schema + "required binary hostAffinity; \n";

      for (MetadataBase.ColumnMetadata column : parquetTableMetadata.getFiles().get(0).getRowGroups().get(0).getColumns()) {
        String name = String.join("_", column.getName());
        schema = schema + "required binary " + name + "_name; \n";
        schema = schema + "required binary " + name + "_minValue; \n";
        schema = schema + "required binary " + name + "_maxValue; \n";
        schema = schema + "required INT64 " + name + "_nulls; \n";
      }
    }
    schema = schema + "} \n";
    return schema;
  }

  public void writeMetadataToParquet(Metadata_V3.ParquetTableMetadata_v3 parquetTableMetadata, Path path, FileSystem fs) throws IOException{
    Stopwatch stopwatch = Stopwatch.createStarted();
    MessageType metadataSchema = MessageTypeParser.parseMessageType(createMetadataSchema(parquetTableMetadata));
    stopwatch.stop();
    logger.info("Took {} ms to create schema", stopwatch.elapsed(TimeUnit.MILLISECONDS));
    stopwatch.reset();
    stopwatch.start();
    ParquetWriter metadataWriter = initWriter(metadataSchema, path, false);
    GroupFactory gf = new SimpleGroupFactory(metadataSchema);
    Gson gson = new Gson();

    for (int i = 0; i < parquetTableMetadata.getFiles().size(); i++) {
      MetadataBase.ParquetFileMetadata parquetFileMetadata = parquetTableMetadata.getFiles().get(i);
      for (int j = 0; j < parquetFileMetadata.getRowGroups().size(); j++) {
        MetadataBase.RowGroupMetadata rowGroup = parquetFileMetadata.getRowGroups().get(j);
        Group simpleGroup = gf.newGroup();
        simpleGroup.append("fid", i)
                .append("path", parquetFileMetadata.getPath())
                .append("length", parquetFileMetadata.getLength())
                .append("start", rowGroup.getStart())
                .append("rgLength", rowGroup.getLength())
                .append("rowCount", rowGroup.getRowCount())
                .append("hostAffinity", gson.toJson(rowGroup.getHostAffinity()));
                for (MetadataBase.ColumnMetadata column : rowGroup.getColumns()) {
                  String name = String.join("_", column.getName());
                  simpleGroup.append(name + "_name", gson.toJson(column.getName()))
                          .append(name + "_minValue", String.valueOf(column.getMinValue()))
                          .append(name + "_maxValue", String.valueOf(column.getMaxValue()))
                          .append(name + "_nulls", column.getNulls());
                }
        metadataWriter.write(simpleGroup);
      }
    }
    metadataWriter.close();
    stopwatch.stop();
    logger.info("Took {} ms to write metadata to parquet", stopwatch.elapsed(TimeUnit.MILLISECONDS));
  }
}
