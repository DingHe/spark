/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.connector.catalog;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.errors.QueryCompilationErrors;
import org.apache.spark.sql.errors.QueryExecutionErrors;
import org.apache.spark.sql.types.StructType;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Catalog methods for working with Tables.
 * <p>
 * TableCatalog implementations may be case sensitive or case insensitive. Spark will pass
 * {@link Identifier table identifiers} without modification. Field names passed to
 * {@link #alterTable(Identifier, TableChange...)} will be normalized to match the case used in the
 * table schema when updating, renaming, or dropping existing columns when catalyst analysis is case
 * insensitive.
 *
 * @since 3.0.0
 */
@Evolving
public interface TableCatalog extends CatalogPlugin {

  /**
   * A reserved property to specify the location of the table. The files of the table
   * should be under this location. The location is a Hadoop Path string.
   */
  //用于指定表的位置。表的数据文件应该存放在这个位置，它的值是一个 Hadoop Path 字符串
  String PROP_LOCATION = "location";

  /**
   * A reserved property to indicate that the table location is managed, not user-specified.
   * If this property is "true", it means it's a managed table even if it has a location. As an
   * example, SHOW CREATE TABLE will not generate the LOCATION clause.
   */
  //表示表的位置是由系统管理的，而不是用户指定的。如果该属性为 true，即使表有一个位置，它也会被认为是一个管理表
  String PROP_IS_MANAGED_LOCATION = "is_managed_location";

  /**
   * A reserved property to specify a table was created with EXTERNAL.
   */
  //指定表是否是外部表。如果该属性为 true，表示该表是外部表
  String PROP_EXTERNAL = "external";

  /**
   * A reserved property to specify the description of the table.
   */
  //指定表的描述信息
  String PROP_COMMENT = "comment";

  /**
   * A reserved property to specify the provider of the table.
   */
  //指定表的提供者，通常指示表的数据存储系统（例如，Hive、Parquet 等）
  String PROP_PROVIDER = "provider";

  /**
   * A reserved property to specify the owner of the table.
   */
  //指定表的所有者
  String PROP_OWNER = "owner";

  /**
   * A prefix used to pass OPTIONS in table properties
   */
  //用于传递表属性中的 OPTIONS 的前缀
  String OPTION_PREFIX = "option.";

  /**
   * @return the set of capabilities for this TableCatalog
   */
  default Set<TableCatalogCapability> capabilities() { return Collections.emptySet(); }

  /**
   * List the tables in a namespace from the catalog.
   * <p>
   * If the catalog supports views, this must return identifiers for only tables and not views.
   *
   * @param namespace a multi-part namespace
   * @return an array of Identifiers for tables
   * @throws NoSuchNamespaceException If the namespace does not exist (optional).
   */
  //列出目录中某个命名空间下的所有表
  Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException;

  /**
   * Load table metadata by {@link Identifier identifier} from the catalog.
   * <p>
   * If the catalog supports views and contains a view for the identifier and not a table, this
   * must throw {@link NoSuchTableException}.
   *
   * @param ident a table identifier
   * @return the table's metadata
   * @throws NoSuchTableException If the table doesn't exist or is a view
   */
  //根据表的标识符从目录中加载表的元数据。如果标识符对应的是一个视图而非表，则抛出异常
  Table loadTable(Identifier ident) throws NoSuchTableException;

  /**
   * Load table metadata by {@link Identifier identifier} from the catalog. Spark will write data
   * into this table later.
   * <p>
   * If the catalog supports views and contains a view for the identifier and not a table, this
   * must throw {@link NoSuchTableException}.
   *
   * @param ident a table identifier
   * @param writePrivileges
   * @return the table's metadata
   * @throws NoSuchTableException If the table doesn't exist or is a view
   *
   * @since 3.5.3
   */
  default Table loadTable(
      Identifier ident,
      Set<TableWritePrivilege> writePrivileges) throws NoSuchTableException {
    return loadTable(ident);
  }

  /**
   * Load table metadata of a specific version by {@link Identifier identifier} from the catalog.
   * <p>
   * If the catalog supports views and contains a view for the identifier and not a table, this
   * must throw {@link NoSuchTableException}.
   *
   * @param ident a table identifier
   * @param version version of the table
   * @return the table's metadata
   * @throws NoSuchTableException If the table doesn't exist or is a view
   */
  default Table loadTable(Identifier ident, String version) throws NoSuchTableException {
    throw QueryCompilationErrors.noSuchTableError(ident);
  }

  /**
   * Load table metadata at a specific time by {@link Identifier identifier} from the catalog.
   * <p>
   * If the catalog supports views and contains a view for the identifier and not a table, this
   * must throw {@link NoSuchTableException}.
   *
   * @param ident a table identifier
   * @param timestamp timestamp of the table, which is microseconds since 1970-01-01 00:00:00 UTC
   * @return the table's metadata
   * @throws NoSuchTableException If the table doesn't exist or is a view
   */
  default Table loadTable(Identifier ident, long timestamp) throws NoSuchTableException {
    throw QueryCompilationErrors.noSuchTableError(ident);
  }

  /**
   * Invalidate cached table metadata for an {@link Identifier identifier}.
   * <p>
   * If the table is already loaded or cached, drop cached data. If the table does not exist or is
   * not cached, do nothing. Calling this method should not query remote services.
   *
   * @param ident a table identifier
   */
  //使表的缓存失效。如果该表已经加载或缓存，它会删除缓存的数据。如果表不存在或没有缓存，则该方法什么都不做
  default void invalidateTable(Identifier ident) {
  }

  /**
   * Test whether a table exists using an {@link Identifier identifier} from the catalog.
   * <p>
   * If the catalog supports views and contains a view for the identifier and not a table, this
   * must return false.
   *
   * @param ident a table identifier
   * @return true if the table exists, false otherwise
   */
  //检查指定标识符的表是否存在。如果表存在，则返回 true，否则返回 false
  default boolean tableExists(Identifier ident) {
    try {
      return loadTable(ident) != null;
    } catch (NoSuchTableException e) {
      return false;
    }
  }

  /**
   * Create a table in the catalog.
   * <p>
   * This is deprecated. Please override
   * {@link #createTable(Identifier, Column[], Transform[], Map)} instead.
   */
  @Deprecated
  Table createTable(
      Identifier ident,
      StructType schema,
      Transform[] partitions,
      Map<String, String> properties) throws TableAlreadyExistsException, NoSuchNamespaceException;

  /**
   * Create a table in the catalog.
   *
   * @param ident a table identifier
   * @param columns the columns of the new table.
   * @param partitions transforms to use for partitioning data in the table
   * @param properties a string map of table properties
   * @return metadata for the new table. This can be null if getting the metadata for the new table
   *         is expensive. Spark will call {@link #loadTable(Identifier)} if needed (e.g. CTAS).
   *
   * @throws TableAlreadyExistsException If a table or view already exists for the identifier
   * @throws UnsupportedOperationException If a requested partition transform is not supported
   * @throws NoSuchNamespaceException If the identifier namespace does not exist (optional)
   */
  //用于创建表，传入的参数包括列、分区转换和属性。如果表已经存在，则抛出 TableAlreadyExistsException。如果分区转换不被支持
  default Table createTable(
      Identifier ident,
      Column[] columns,
      Transform[] partitions,
      Map<String, String> properties) throws TableAlreadyExistsException, NoSuchNamespaceException {
    return createTable(ident, CatalogV2Util.v2ColumnsToStructType(columns), partitions, properties);
  }

  /**
   * If true, mark all the fields of the query schema as nullable when executing
   * CREATE/REPLACE TABLE ... AS SELECT ... and creating the table.
   */
  //确定在执行 CREATE/REPLACE TABLE ... AS SELECT ... 时，查询模式的所有字段是否应标记为可空字段。默认情况下，返回 true，即所有字段为可空
  default boolean useNullableQuerySchema() {
    return true;
  }

  /**
   * Apply a set of {@link TableChange changes} to a table in the catalog.
   * <p>
   * Implementations may reject the requested changes. If any change is rejected, none of the
   * changes should be applied to the table.
   * <p>
   * The requested changes must be applied in the order given.
   * <p>
   * If the catalog supports views and contains a view for the identifier and not a table, this
   * must throw {@link NoSuchTableException}.
   *
   * @param ident a table identifier
   * @param changes changes to apply to the table
   * @return updated metadata for the table. This can be null if getting the metadata for the
   *         updated table is expensive. Spark always discard the returned table here.
   *
   * @throws NoSuchTableException If the table doesn't exist or is a view
   * @throws IllegalArgumentException If any change is rejected by the implementation.
   */
  //应用一组表更改操作。每个操作必须按给定的顺序应用。如果有任何更改被拒绝，则不会对表进行任何更改。
  // 如果表不存在或是视图，则抛出 NoSuchTableException
  Table alterTable(
      Identifier ident,
      TableChange... changes) throws NoSuchTableException;

  /**
   * Drop a table in the catalog.
   * <p>
   * If the catalog supports views and contains a view for the identifier and not a table, this
   * must not drop the view and must return false.
   *
   * @param ident a table identifier
   * @return true if a table was deleted, false if no table exists for the identifier
   */
  boolean dropTable(Identifier ident);

  /**
   * Drop a table in the catalog and completely remove its data by skipping a trash even if it is
   * supported.
   * <p>
   * If the catalog supports views and contains a view for the identifier and not a table, this
   * must not drop the view and must return false.
   * <p>
   * If the catalog supports to purge a table, this method should be overridden.
   * The default implementation throws {@link UnsupportedOperationException}.
   *
   * @param ident a table identifier
   * @return true if a table was deleted, false if no table exists for the identifier
   * @throws UnsupportedOperationException If table purging is not supported
   *
   * @since 3.1.0
   */
  //用于删除表并完全移除其数据，跳过回收站（如果支持的话）
  default boolean purgeTable(Identifier ident) throws UnsupportedOperationException {
    throw QueryExecutionErrors.unsupportedPurgeTableError();
  }

  /**
   * Renames a table in the catalog.
   * <p>
   * If the catalog supports views and contains a view for the old identifier and not a table, this
   * throws {@link NoSuchTableException}. Additionally, if the new identifier is a table or a view,
   * this throws {@link TableAlreadyExistsException}.
   * <p>
   * If the catalog does not support table renames between namespaces, it throws
   * {@link UnsupportedOperationException}.
   *
   * @param oldIdent the table identifier of the existing table to rename
   * @param newIdent the new table identifier of the table
   * @throws NoSuchTableException If the table to rename doesn't exist or is a view
   * @throws TableAlreadyExistsException If the new table name already exists or is a view
   * @throws UnsupportedOperationException If the namespaces of old and new identifiers do not
   *                                       match (optional)
   */
  //用于重命名表。如果目录中存在视图而非表
  void renameTable(Identifier oldIdent, Identifier newIdent)
      throws NoSuchTableException, TableAlreadyExistsException;
}
