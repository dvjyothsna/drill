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
import com.fasterxml.jackson.databind.KeyDeserializer;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.drill.common.expression.SchemaPath;
import static org.apache.drill.exec.store.parquet.metadata.MetadataVersion.Constants.V3_3;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
public class Metadata_Summary {

  @JsonTypeName(V3_3)
  public static class Summary {

//    @JsonProperty(value = "metadata_version", access = JsonProperty.Access.WRITE_ONLY) private String metadataVersion;
    @JsonProperty(value = "metadata_version") private String metadataVersion;

    /*
    ColumnTypeInfo is schema information from all the files and row groups, merged into
    one. To get this info, we pass the ParquetTableMetadata object all the way dow to the
    RowGroup and the column type is built there as it is read from the footer.
    */
    @JsonProperty public ConcurrentHashMap<Metadata_V3.ColumnTypeMetadata_v3.Key, Metadata_V3.ColumnTypeMetadata_v3> columnTypeInfo;
    @JsonProperty List<String> directories;
    @JsonProperty String drillVersion;

    public Summary() {

    }

    public Summary(String metadataVersion, List<String> directories, ConcurrentHashMap<Metadata_V3.ColumnTypeMetadata_v3.Key, Metadata_V3.ColumnTypeMetadata_v3> columnTypeInfo, String drillVersion) {
      this.metadataVersion = metadataVersion;
      this.directories = directories;
      this.columnTypeInfo = columnTypeInfo;
      this.drillVersion = drillVersion;
    }

    public void setMetadataVersion(String metadataVersion) {
      this.metadataVersion = metadataVersion;
    }

    public ConcurrentHashMap<Metadata_V3.ColumnTypeMetadata_v3.Key, Metadata_V3.ColumnTypeMetadata_v3> getColumnTypeInfo() {
      return columnTypeInfo;
    }

    public void setColumnTypeInfo(ConcurrentHashMap<Metadata_V3.ColumnTypeMetadata_v3.Key, Metadata_V3.ColumnTypeMetadata_v3> columnTypeInfo) {
      this.columnTypeInfo = columnTypeInfo;
    }

    public void setDirectories(List<String> directories) {
      this.directories = directories;
    }

    public void setDrillVersion(String drillVersion) {
      this.drillVersion = drillVersion;
    }

    public Metadata_V3.ColumnTypeMetadata_v3 getColumnTypeInfo(String[] name) {
    return columnTypeInfo.get(new ColumnTypeMetadata_v3.Key(name));
  }

  @JsonIgnore
   public List<String> getDirectories() {
    return directories;
  }

  @JsonIgnore public String getMetadataVersion() {
    return metadataVersion;
  }

    /**
     * If directories list and file metadata list contain relative paths, update it to absolute ones
     * @param baseDir base parent directory
     */
    @JsonIgnore public void updateRelativePaths(String baseDir) {
      // update directories paths to absolute ones
      this.directories = MetadataPathUtils.convertToAbsolutePaths(directories, baseDir);
    }

     public boolean hasColumnMetadata() {
      return true;
    }

    @JsonIgnore  public PrimitiveType.PrimitiveTypeName getPrimitiveType(String[] columnName) {
      return getColumnTypeInfo(columnName).primitiveType;
    }

    @JsonIgnore  public OriginalType getOriginalType(String[] columnName) {
      return getColumnTypeInfo(columnName).originalType;
    }

    @JsonIgnore
    public Integer getRepetitionLevel(String[] columnName) {
      return getColumnTypeInfo(columnName).repetitionLevel;
    }

    @JsonIgnore
    public Integer getDefinitionLevel(String[] columnName) {
      return getColumnTypeInfo(columnName).definitionLevel;
    }

    @JsonIgnore
    public boolean isRowGroupPrunable() {
      return true;
    }

    @JsonIgnore
    public String getDrillVersion() {
      return drillVersion;
    }
  }
  public static class ColumnTypeMetadata_v3 {
    @JsonProperty public String[] name;
    @JsonProperty public PrimitiveType.PrimitiveTypeName primitiveType;
    @JsonProperty public OriginalType originalType;
    @JsonProperty public int precision;
    @JsonProperty public int scale;
    @JsonProperty public int repetitionLevel;
    @JsonProperty public int definitionLevel;

    public Key getKey() {
      return key;
    }

    // Key to find by name only
    @JsonIgnore private Key key;

    public ColumnTypeMetadata_v3() {
    }

    @JsonIgnore private Key key() {
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

      @Override public int hashCode() {
        if (hashCode == 0) {
          hashCode = name.hashCode();
        }
        return hashCode;
      }

      @Override public boolean equals(Object obj) {
        if (obj == null) {
          return false;
        }
        if (getClass() != obj.getClass()) {
          return false;
        }
        final Key other = (Key) obj;
        return this.name.equals(other.name);
      }

      @Override public String toString() {
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
}
