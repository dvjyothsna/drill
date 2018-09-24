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

    String schema = "message metadata { \n";
    schema = schema + "required INT32 fid; \n";
    schema = schema + "required binary path; \n";
    schema = schema + "required INT64 length; \n";
    schema = schema + "required INT64 start; \n";
    schema = schema + "required INT64 rgLength; \n";
    schema = schema + "required INT64 rowCount; \n";
    schema = schema + "required binary hostAffinity; \n";
    schema = schema + "required int32 rgid; \n";

    for ( MetadataBase.ColumnMetadata column : parquetTableMetadata.getFiles().get(0).getRowGroups().get(0).getColumns()) {
//      final CharsetEncoder charsetEncoder = Charsets.UTF_8.newEncoder();
//      final CharBuffer patternCharBuffer = CharBuffer.wrap(java.util.Arrays.toString(column.getName()));
//      try {
//        String nameEncoded = String.valueOf(charsetEncoder.encode(patternCharBuffer));
//      } catch (CharacterCodingException e) {
//        e.printStackTrace();
//      }

      String name = java.util.Arrays.toString(column.getName());
      schema = schema + "required binary "+ name + "_name; \n";
      schema = schema + "required binary " + name + "_minValue; \n";
      schema = schema + "required binary " + name + "_maxValue; \n";
      schema = schema + "required INT64 " + name + "_nulls; \n";
    }
    schema = schema + "} \n";
    return schema;
  }

  public void writeMetadataToParquet(Metadata_V3.ParquetTableMetadata_v3 parquetTableMetadata, Path path, FileSystem fs) throws IOException{
    MessageType metadataSchema = MessageTypeParser.parseMessageType(createMetadataSchema(parquetTableMetadata));
    ParquetWriter metadataWriter = initWriter(metadataSchema, path, false);
    GroupFactory gf = new SimpleGroupFactory(metadataSchema);
    Gson gson = new Gson();
    String columnTypeInfo = gson.toJson(parquetTableMetadata.columnTypeInfo);
    String files = gson.toJson(parquetTableMetadata.getFiles());
    String directories = gson.toJson(parquetTableMetadata.getDirectories());
    String drillVersion = gson.toJson(parquetTableMetadata.getDrillVersion());

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
                .append("hostAffinity", gson.toJson(rowGroup.getHostAffinity()))
                .append("rgid", j);
                for (MetadataBase.ColumnMetadata column : rowGroup.getColumns()) {
                  String name = java.util.Arrays.toString(column.getName());
                  simpleGroup.append(name + "_name", name)
                          .append(name + "_minValue", String.valueOf(column.getMinValue()))
                          .append(name + "_maxValue", String.valueOf(column.getMaxValue()))
                          .append(name + "_nulls", column.getNulls());
                }
        metadataWriter.write(simpleGroup);
      }
    }
//    simpleGroup.append("metadata_version", parquetTableMetadata.getMetadataVersion())
//            .append("columnTypeInfo", columnTypeInfo)
//            .append("files", files)
//            .append("directories", directories)
//            .append("drillVersion", drillVersion);
    metadataWriter.close();
  }
}
