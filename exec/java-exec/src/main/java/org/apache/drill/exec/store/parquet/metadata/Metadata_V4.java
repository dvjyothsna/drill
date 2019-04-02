/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.parquet.metadata;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.drill.common.expression.SchemaPath;

import static org.apache.drill.exec.store.parquet.metadata.MetadataBase.ColumnMetadata;
import static org.apache.drill.exec.store.parquet.metadata.MetadataBase.ParquetFileMetadata;
import static org.apache.drill.exec.store.parquet.metadata.MetadataBase.ParquetTableMetadataBase;
import static org.apache.drill.exec.store.parquet.metadata.MetadataBase.RowGroupMetadata;
import static org.apache.drill.exec.store.parquet.metadata.MetadataVersion.Constants.V4;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;

public class Metadata_V4 {

  public static class ParquetTableMetadata_v4 extends ParquetTableMetadataBase {

    MetadataSummary metadataSummary = new MetadataSummary();
    FileMetadata fileMetadata = new FileMetadata();

    public ParquetTableMetadata_v4(MetadataSummary metadataSummary) {
      this.metadataSummary = metadataSummary;
    }

    public ParquetTableMetadata_v4(MetadataSummary metadataSummary, FileMetadata fileMetadata) {
      this.metadataSummary = metadataSummary;
      this.fileMetadata = fileMetadata;
    }

    public ParquetTableMetadata_v4(String metadataVersion, ParquetTableMetadataBase parquetTableMetadata,
                                   List<ParquetFileMetadata_v4> files, List<Path> directories, String drillVersion, long totalRowCount, boolean allColumnsInteresting) {
      this.metadataSummary.metadataVersion = metadataVersion;
      this.fileMetadata.files = files;
      this.metadataSummary.directories = directories;
      this.metadataSummary.columnTypeInfo = ((ParquetTableMetadata_v4) parquetTableMetadata).metadataSummary.columnTypeInfo;
      this.metadataSummary.drillVersion = drillVersion;
      this.metadataSummary.totalRowCount = totalRowCount;
      this.metadataSummary.allColumnsInteresting = allColumnsInteresting;
    }

    public ColumnTypeMetadata_v4 getColumnTypeInfo(String[] name) {
      return metadataSummary.getColumnTypeInfo(name);
    }

    @JsonIgnore
    @Override
    public List<Path> getDirectories() {
      return metadataSummary.getDirectories();
    }

    @Override
    public List<? extends ParquetFileMetadata> getFiles() {
      return fileMetadata.getFiles();
    }

    @Override
    public String getMetadataVersion() {
      return metadataSummary.getMetadataVersion();
    }

    /**
     * If directories list and file metadata list contain relative paths, update it to absolute ones
     *
     * @param baseDir base parent directory
     */
    public void updateRelativePaths(String baseDir) {
      // update directories paths to absolute ones
      this.metadataSummary.directories = MetadataPathUtils.convertToAbsolutePaths(metadataSummary.directories, baseDir);

      // update files paths to absolute ones
      this.fileMetadata.files = MetadataPathUtils.convertToFilesWithAbsolutePathsForV4(fileMetadata.files, baseDir);
    }

    @Override
    public void assignFiles(List<? extends ParquetFileMetadata> newFiles) {
      this.fileMetadata.assignFiles(newFiles);
    }

    @Override
    public boolean hasColumnMetadata() {
      return true;
    }

    @Override
    public PrimitiveType.PrimitiveTypeName getPrimitiveType(String[] columnName) {
      return getColumnTypeInfo(columnName).primitiveType;
    }

    @Override
    public OriginalType getOriginalType(String[] columnName) {
      return getColumnTypeInfo(columnName).originalType;
    }

    @Override
    public Integer getRepetitionLevel(String[] columnName) {
      return getColumnTypeInfo(columnName).repetitionLevel;
    }

    @Override
    public Integer getDefinitionLevel(String[] columnName) {
      return getColumnTypeInfo(columnName).definitionLevel;
    }

    @Override
    public Integer getScale(String[] columnName) {
      return getColumnTypeInfo(columnName).scale;
    }

    @Override
    public Integer getPrecision(String[] columnName) {
      return getColumnTypeInfo(columnName).precision;
    }

    @Override
    public boolean isRowGroupPrunable() {
      return true;
    }

    @Override
    public ParquetTableMetadataBase clone() {
      return new ParquetTableMetadata_v4(metadataSummary, fileMetadata);
    }

    @Override
    public String getDrillVersion() {
      return metadataSummary.drillVersion;
    }


    public MetadataSummary getSummary() {
      return metadataSummary;
    }


    public long getTotalRowCount() {
      return metadataSummary.getTotalRowCount();
    }


    public long getTotalNullCount(String[] columnName) {
      return getColumnTypeInfo(columnName).totalNullCount;
    }


    public boolean isAllColumnsInteresting() {
      return metadataSummary.isAllColumnsInteresting();
    }

    public ConcurrentHashMap<ColumnTypeMetadata_v4.Key, ColumnTypeMetadata_v4> getColumnTypeInfoMap() {
      return metadataSummary.columnTypeInfo;
    }

    public void setTotalRowCount(long totalRowCount) {
      metadataSummary.setTotalRowCount(totalRowCount);
    }

    public void setAllColumnsInteresting(boolean allColumnsInteresting) {
      metadataSummary.allColumnsInteresting = allColumnsInteresting;
    }

  }

  /**
   * Struct which contains the metadata for a single parquet file
   */
  public static class ParquetFileMetadata_v4 extends ParquetFileMetadata {
    @JsonProperty
    public Path path;
    @JsonProperty
    public Long length;
    @JsonProperty
    public List<RowGroupMetadata_v4> rowGroups;

    public ParquetFileMetadata_v4() {

    }

    public ParquetFileMetadata_v4(Path path, Long length, List<RowGroupMetadata_v4> rowGroups) {
      this.path = path;
      this.length = length;
      this.rowGroups = rowGroups;
    }

    @Override
    public String toString() {
      return String.format("path: %s rowGroups: %s", path, rowGroups);
    }

    @JsonIgnore
    @Override
    public Path getPath() {
      return path;
    }

    @JsonIgnore
    @Override
    public Long getLength() {
      return length;
    }

    @JsonIgnore
    @Override
    public List<? extends RowGroupMetadata> getRowGroups() {
      return rowGroups;
    }
  }


  /**
   * A struct that contains the metadata for a parquet row group
   */
  public static class RowGroupMetadata_v4 extends RowGroupMetadata {
    @JsonProperty
    public Long start;
    @JsonProperty
    public Long length;
    @JsonProperty
    public Long rowCount;
    @JsonProperty
    public Map<String, Float> hostAffinity;
    @JsonProperty
    public List<ColumnMetadata_v4> columns;

    public RowGroupMetadata_v4() {
    }

    public RowGroupMetadata_v4(Long start, Long length, Long rowCount, Map<String, Float> hostAffinity,
                               List<ColumnMetadata_v4> columns) {
      this.start = start;
      this.length = length;
      this.rowCount = rowCount;
      this.hostAffinity = hostAffinity;
      this.columns = columns;
    }

    @Override
    public Long getStart() {
      return start;
    }

    @Override
    public Long getLength() {
      return length;
    }

    @Override
    public Long getRowCount() {
      return rowCount;
    }

    @Override
    public Map<String, Float> getHostAffinity() {
      return hostAffinity;
    }

    @Override
    public List<? extends ColumnMetadata> getColumns() {
      return columns;
    }
  }


  public static class ColumnTypeMetadata_v4 {
    @JsonProperty
    public String[] name;
    @JsonProperty
    public PrimitiveType.PrimitiveTypeName primitiveType;
    @JsonProperty
    public OriginalType originalType;
    @JsonProperty
    public int precision;
    @JsonProperty
    public int scale;
    @JsonProperty
    public int repetitionLevel;
    @JsonProperty
    public int definitionLevel;
    @JsonProperty
    public long totalNullCount = 0;
    @JsonProperty
    public boolean isInteresting = false;

    // Key to find by name only
    @JsonIgnore
    private Key key;

    public ColumnTypeMetadata_v4() {
    }

    public ColumnTypeMetadata_v4(String[] name, PrimitiveType.PrimitiveTypeName primitiveType, OriginalType originalType, int precision, int scale, int repetitionLevel, int definitionLevel, long totalNullCount, boolean isInteresting) {
      this.name = name;
      this.primitiveType = primitiveType;
      this.originalType = originalType;
      this.precision = precision;
      this.scale = scale;
      this.repetitionLevel = repetitionLevel;
      this.definitionLevel = definitionLevel;
      this.key = new Key(name);
      this.totalNullCount = totalNullCount;
      this.isInteresting = isInteresting;
    }

    @JsonIgnore
    private Key key() {
      return this.key;
    }

    public static class Key {
      private SchemaPath name;
      private int hashCode = 0;

      public Key(String[] name) {
        this.name = SchemaPath.getCompoundPath(name);
      }

      public Key(SchemaPath name) {
        this.name = new SchemaPath(name);
      }

      @Override
      public int hashCode() {
        if (hashCode == 0) {
          hashCode = name.hashCode();
        }
        return hashCode;
      }

      @Override
      public boolean equals(Object obj) {
        if (obj == null) {
          return false;
        }
        if (getClass() != obj.getClass()) {
          return false;
        }
        final Key other = (Key) obj;
        return this.name.equals(other.name);
      }

      @Override
      public String toString() {
        return name.toString();
      }

      public static class DeSerializer extends KeyDeserializer {

        public DeSerializer() {
        }

        @Override
        public Object deserializeKey(String key, com.fasterxml.jackson.databind.DeserializationContext ctxt) {
          // key string should contain '`' char if the field was serialized as SchemaPath object
          if (key.contains("`")) {
            return new Key(SchemaPath.parseFromString(key));
          }
          return new Key(key.split("\\."));
        }
      }
    }
  }


  /**
   * A struct that contains the metadata for a column in a parquet file
   */
  public static class ColumnMetadata_v4 extends ColumnMetadata {
    // Use a string array for name instead of Schema Path to make serialization easier
    @JsonProperty
    public String[] name;
    @JsonProperty
    public Long nulls;

    public Object minValue;
    public Object maxValue;

    @JsonIgnore
    private PrimitiveType.PrimitiveTypeName primitiveType;

    public ColumnMetadata_v4() {
    }

    public ColumnMetadata_v4(String[] name, PrimitiveType.PrimitiveTypeName primitiveType, Object minValue, Object maxValue, Long nulls) {
      this.name = name;
      this.minValue = minValue;
      this.maxValue = maxValue;
      this.nulls = nulls;
      this.primitiveType = primitiveType;
    }

    @JsonProperty(value = "minValue")
    public void setMin(Object minValue) {
      this.minValue = minValue;
    }

    @JsonProperty(value = "maxValue")
    public void setMax(Object maxValue) {
      this.maxValue = maxValue;
    }

    @Override
    public String[] getName() {
      return name;
    }

    @Override
    public Long getNulls() {
      return nulls;
    }

    /**
     * Checks that the column chunk has a single value.
     * Returns {@code true} if {@code minValue} and {@code maxValue} are the same but not null
     * and nulls count is 0 or equal to the rows count.
     * <p>
     * Returns {@code true} if {@code minValue} and {@code maxValue} are null and the number of null values
     * in the column chunk is equal to the rows count.
     * <p>
     * Comparison of nulls and rows count is needed for the cases:
     * <ul>
     * <li>column with primitive type has single value and null values</li>
     *
     * <li>column <b>with primitive type</b> has only null values, min/max couldn't be null,
     * but column has single value</li>
     * </ul>
     *
     * @param rowCount rows count in column chunk
     * @return true if column has single value
     */
    @Override
    public boolean hasSingleValue(long rowCount) {
      if (isNumNullsSet()) {
        if (minValue != null) {
          // Objects.deepEquals() is used here, since min and max may be byte arrays
          return (nulls == 0 || nulls == rowCount) && Objects.deepEquals(minValue, maxValue);
        } else {
          return nulls == rowCount && maxValue == null;
        }
      }
      return false;
    }

    @Override
    public Object getMinValue() {
      return minValue;
    }

    @Override
    public Object getMaxValue() {
      return maxValue;
    }

    @Override
    public PrimitiveType.PrimitiveTypeName getPrimitiveType() {
      return null;
    }

    @Override
    public OriginalType getOriginalType() {
      return null;
    }

    // We use a custom serializer and write only non null values.
    public static class Serializer extends JsonSerializer<ColumnMetadata_v4> {
      @Override
      public void serialize(ColumnMetadata_v4 value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
        jgen.writeStartObject();
        jgen.writeArrayFieldStart("name");
        for (String n : value.name) {
          jgen.writeString(n);
        }
        jgen.writeEndArray();
        if (value.minValue != null) {
          Object val;
          if (value.primitiveType == PrimitiveType.PrimitiveTypeName.BINARY
                  || value.primitiveType == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
            val = ((Binary) value.minValue).getBytes();
          } else {
            val = value.minValue;
          }
          jgen.writeObjectField("minValue", val);
        }
        if (value.maxValue != null) {
          Object val;
          if (value.primitiveType == PrimitiveType.PrimitiveTypeName.BINARY
                  || value.primitiveType == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
            val = ((Binary) value.maxValue).getBytes();
          } else {
            val = value.maxValue;
          }
          jgen.writeObjectField("maxValue", val);
        }

        if (value.nulls != null) {
          jgen.writeObjectField("nulls", value.nulls);
        }
        jgen.writeEndObject();
      }
    }
  }

  @JsonTypeName(V4)
  public static class MetadataSummary {

    @JsonProperty(value = "metadata_version")
    private String metadataVersion;
    /*
     ColumnTypeInfo is schema information from all the files and row groups, merged into
     one. To get this info, we pass the ParquetTableMetadata object all the way down to the
     RowGroup and the column type is built there as it is read from the footer.
     */
    @JsonProperty
    public ConcurrentHashMap<ColumnTypeMetadata_v4.Key, ColumnTypeMetadata_v4> columnTypeInfo;
    @JsonProperty
    List<Path> directories;
    @JsonProperty
    String drillVersion;
    @JsonProperty
    long totalRowCount = 0;
    @JsonProperty
    boolean allColumnsInteresting = false;

    public MetadataSummary() {

    }

    public MetadataSummary(String metadataVersion, String drillVersion) {
      this.metadataVersion = metadataVersion;
      this.drillVersion = drillVersion;
    }

    public MetadataSummary(String metadataVersion, String drillVersion, List<Path> directories) {
      this.metadataVersion = metadataVersion;
      this.drillVersion = drillVersion;
      this.directories = directories;
    }

    public ColumnTypeMetadata_v4 getColumnTypeInfo(String[] name) {
      return columnTypeInfo.get(new ColumnTypeMetadata_v4.Key(name));
    }

    public ColumnTypeMetadata_v4 getColumnTypeInfo(ColumnTypeMetadata_v4.Key key) {
      return columnTypeInfo.get(key);
    }

    public List<Path> getDirectories() {
      return directories;
    }

    @JsonIgnore
    public String getMetadataVersion() {
      return metadataVersion;
    }

    public boolean isAllColumnsInteresting() {
      return allColumnsInteresting;
    }

    public void setAllColumnsInteresting(boolean allColumnsInteresting) {
      this.allColumnsInteresting = allColumnsInteresting;
    }

    public void setTotalRowCount(Long totalRowCount) {
      this.totalRowCount = totalRowCount;
    }

    public Long getTotalRowCount() {
      return this.totalRowCount;
    }
  }

  /*
   * A struct that holds list of file metadata in a directory
   */
  public static class FileMetadata {

    @JsonProperty
    List<ParquetFileMetadata_v4> files;

    public FileMetadata() {
    }

    @JsonIgnore
    public List<ParquetFileMetadata_v4> getFiles() {
      return files;
    }

    @JsonIgnore
    public void assignFiles(List<? extends ParquetFileMetadata> newFiles) {
      this.files = (List<ParquetFileMetadata_v4>) newFiles;
    }
  }

  /*
   * A struct that holds file metadata and row count and null count of a single file
   */
  public static class ParquetFileAndRowCountMetadata {

    ParquetFileMetadata_v4 fileMetadata;
    Map<ColumnTypeMetadata_v4.Key, Long> totalNullCountMap;
    long fileRowCount;

    public ParquetFileAndRowCountMetadata() {
    }

    public ParquetFileAndRowCountMetadata(ParquetFileMetadata_v4 fileMetadata, Map<ColumnTypeMetadata_v4.Key, Long> totalNullCountMap, long fileRowCount) {
      this.fileMetadata = fileMetadata;
      this.totalNullCountMap = totalNullCountMap;
      this.fileRowCount = fileRowCount;
    }

    public ParquetFileMetadata_v4 getFileMetadata() {
      return this.fileMetadata;
    }

    public long getFileRowCount() {
      return this.fileRowCount;
    }

    public Map<ColumnTypeMetadata_v4.Key, Long> getTotalNullCountMap() {
      return totalNullCountMap;
    }

  }
}
