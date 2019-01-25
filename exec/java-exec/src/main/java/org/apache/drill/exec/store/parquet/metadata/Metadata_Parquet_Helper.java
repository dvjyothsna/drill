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
    StringBuilder schema = new StringBuilder();
    if (parquetTableMetadata  != null && parquetTableMetadata.getFiles().size() != 0) {
      schema.append("message metadata { \n");
      schema.append("required INT32 fid; \n");
      schema.append("required binary path; \n");
      schema.append("required INT64 length; \n");
      schema.append("required INT64 start; \n");
      schema.append("required INT64 rgLength; \n");
      schema.append("required INT64 rowCount; \n");
      schema.append("required binary hostAffinity; \n");

//      for (MetadataBase.ColumnMetadata column : parquetTableMetadata.getFiles().get(0).getRowGroups().get(0).getColumns()) {
//        String[] columnName = column.getName();
//        String name = String.join(":", column.getName());
//        schema.append("required binary " + name + "_name; \n");
////        schema.append("required binary " + name + "_columnStats; \n");
//
//       /* TypeProtos.MajorType type = ParquetReaderUtility.getType(parquetTableMetadata.getPrimitiveType(columnName), parquetTableMetadata.getOriginalType(columnName), 0, 0);
//        switch (type.getMinorType()) {
//          case INT:
//          case TIME:
//            schema.append("required INT32 " + name + "_minValue; \n");
//            schema.append("required INT32 " + name + "_maxValue; \n");
//            break;
//          case BIGINT:
//          case TIMESTAMP:
//            schema.append("required INT64 " + name + "_minValue; \n");
//            schema.append("required INT64 " + name + "_maxValue; \n");
//            break;
//          case FLOAT4:
//            schema.append("required FLOAT " + name + "_minValue; \n");
//            schema.append("required FLOAT " + name + "_maxValue; \n");
//            break;
//          case FLOAT8:
//            schema.append("required DOUBLE " + name + "_minValue; \n");
//            schema.append("required DOUBLE " + name + "_maxValue; \n");
//            break;
//          case BIT:
//            schema.append("required BOOLEAN " + name + "_minValue; \n");
//            schema.append("required BOOLEAN " + name + "_maxValue; \n");
//            break;
//          default:
//            schema.append("required binary " + name + "_minValue; \n");
//            schema.append("required binary " + name + "_maxValue; \n");
//        }*/
//        schema.append("required binary " + name + "_minValue; \n");
//        schema.append("required binary " + name + "_maxValue; \n");
//        schema.append("required INT64 " + name + "_nulls; \n");
//      }
    }
    schema.append("} \n");
    return schema.toString();
  }

  public void writeMetadataToParquet(Metadata_V3.ParquetTableMetadata_v3 parquetTableMetadata, Path path, FileSystem fs) throws IOException{
    Stopwatch stopwatch = Stopwatch.createStarted();
    MessageType metadataSchema = MessageTypeParser.parseMessageType(createMetadataSchema(parquetTableMetadata));
    stopwatch.stop();
    logger.info("Took {} ms to create schema", stopwatch.elapsed(TimeUnit.MILLISECONDS));
    stopwatch.reset();
    ParquetWriter metadataWriter = initWriter(metadataSchema, path, true);
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
//                for (MetadataBase.ColumnMetadata column : rowGroup.getColumns()) {
//                  String[] columnName = column.getName();
//                  String name = String.join(":", columnName);
//                  simpleGroup.append(name + "_name", name);
////                  simpleGroup.append(name + "_columnStats", gson.toJson(column));
//                  simpleGroup.append(name + "_minValue", String.valueOf(column.getMinValue()));
//                  simpleGroup.append(name + "_maxValue", String.valueOf(column.getMaxValue()));
//               /*   TypeProtos.MajorType type = ParquetReaderUtility.getType(parquetTableMetadata.getPrimitiveType(columnName), parquetTableMetadata.getOriginalType(columnName), 0, 0);
//                  switch (type.getMinorType()) {
//                    case INT:
//                    case TIME:
//                      simpleGroup.append(name + "_minValue", Integer.parseInt(String.valueOf(column.getMinValue())));
//                      simpleGroup.append(name + "_maxValue", Integer.parseInt(String.valueOf(column.getMaxValue())));
//                      break;
//                    case BIGINT:
//                    case TIMESTAMP:
//                      simpleGroup.append(name + "_minValue", Long.parseLong(String.valueOf(column.getMinValue())));
//                      simpleGroup.append(name + "_maxValue", Long.parseLong(String.valueOf(column.getMaxValue())));
//                      break;
//                    case FLOAT4:
//                      simpleGroup.append(name + "_minValue", Float.parseFloat(String.valueOf(column.getMinValue())));
//                      simpleGroup.append(name + "_maxValue", Float.parseFloat(String.valueOf(column.getMaxValue())));
//                      break;
//                    case FLOAT8:
//                      simpleGroup.append(name + "_minValue", Double.parseDouble(String.valueOf(column.getMinValue())));
//                      simpleGroup.append(name + "_maxValue", Double.parseDouble(String.valueOf(column.getMaxValue())));
//                      break;
//                    case BIT:
//                      simpleGroup.append(name + "_minValue", Boolean.parseBoolean(String.valueOf(column.getMinValue())));
//                      simpleGroup.append(name + "_maxValue", Boolean.parseBoolean(String.valueOf(column.getMaxValue())));
//                      break;
//                    default:
//                      simpleGroup.append(name + "_minValue", String.valueOf(column.getMinValue()));
//                      simpleGroup.append(name + "_maxValue", String.valueOf(column.getMaxValue()));
//                  } */
//                  simpleGroup.append(name + "_nulls", column.getNulls());
//
//                }
        metadataWriter.write(simpleGroup);
      }
    }
    stopwatch.start();
    metadataWriter.close();
    stopwatch.stop();
    logger.info("Took {} ms to write metadata to parquet", stopwatch.elapsed(TimeUnit.MILLISECONDS));
  }
}
