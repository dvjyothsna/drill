package org.apache.drill.exec.store.parquet.metadata;

import com.google.gson.Gson;
import java.io.IOException;
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
  public static String metadataSchemaMsg =
          "message metadata { \n" +
                  "  required binary metadata_version; \n" +
                  "  required binary columnTypeInfo; \n" +
                  "  required binary files; \n" +
                  "  required binary directories; \n" +
                  "  required binary drillVersion; \n" +
                  "} \n";
  public static MessageType metadataSchema = MessageTypeParser.parseMessageType(metadataSchemaMsg);

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

  public void writeMetadataToParquet(Metadata_V3.ParquetTableMetadata_v3 parquetTableMetadata, Path path, FileSystem fs) throws IOException{
    ParquetWriter metadataWriter = initWriter(metadataSchema, path, false);
    GroupFactory gf = new SimpleGroupFactory(metadataSchema);

    Group simpleGroup = gf.newGroup();
    Gson gson = new Gson();
    String columnTypeInfo = gson.toJson(parquetTableMetadata.columnTypeInfo);
    String files = gson.toJson(parquetTableMetadata.getFiles());
    String directories = gson.toJson(parquetTableMetadata.getDirectories());
    String drillVersion = gson.toJson(parquetTableMetadata.getDrillVersion());

    simpleGroup.append("metadata_version", parquetTableMetadata.getMetadataVersion())
            .append("columnTypeInfo", columnTypeInfo)
            .append("files", files)
            .append("directories", directories)
            .append("drillVersion", drillVersion);
    metadataWriter.write(simpleGroup);
    metadataWriter.close();
  }
}
