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

package org.apache.spark.sql.connector.catalog

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}

/**
 * A trait to encapsulate catalog lookup function and helpful extractors.
 */
// 它主要负责从多部分的标识符（通常是表、视图、函数等的名称）中解析和查找不同类型的 CatalogPlugin（即 Spark 中的目录插件）。
// 它包含了多个帮助性的提取器（extractors），用于从多部分标识符中提取出相关的 catalog 和 identifier，并做一些必要的查找和处理
private[sql] trait LookupCatalog extends Logging {

  protected val catalogManager: CatalogManager

  /**
   * Returns the current catalog set.
   */
    //获取当前的 CatalogPlugin，它代表了当前 Spark 会话中使用的目录插件
  def currentCatalog: CatalogPlugin = catalogManager.currentCatalog

  /**
   * Extract catalog plugin and remaining identifier names.
   *
   * This does not substitute the default catalog if no catalog is set in the identifier.
   */
  //该提取器尝试将标识符分解为一个可选的 CatalogPlugin 和剩余的标识符部分
  private object CatalogAndMultipartIdentifier {
    def unapply(parts: Seq[String]): Some[(Option[CatalogPlugin], Seq[String])] = parts match {
      case Seq(_) =>
        Some((None, parts))  //如果标识符只有一个部分，则没有提供 catalog 名称，返回 None
      //如果标识符的第一个部分是 catalog 名称，则尝试获取对应的 CatalogPlugin，并将剩余部分作为标识符返回。如果找不到该 catalog，则返回 None
      case Seq(catalogName, tail @ _*) =>
        try {
          Some((Some(catalogManager.catalog(catalogName)), tail))
        } catch {
          case _: CatalogNotFoundException =>
            Some((None, parts))
        }
    }
  }

  /**
   * Extract session catalog and identifier from a multi-part identifier.
   */
  //该提取器将一个多部分的标识符分解为 session catalog 和 identifier（通常用于从会话级别的目录中查找）
  object SessionCatalogAndIdentifier {

    def unapply(parts: Seq[String]): Option[(CatalogPlugin, Identifier)] = parts match {
      case CatalogAndIdentifier(catalog, ident) if CatalogV2Util.isSessionCatalog(catalog) =>
        Some(catalog, ident)
      case _ => None
    }
  }

  /**
   * Extract non-session catalog and identifier from a multi-part identifier.
   */
  object NonSessionCatalogAndIdentifier {
    def unapply(parts: Seq[String]): Option[(CatalogPlugin, Identifier)] = parts match {
      case CatalogAndIdentifier(catalog, ident) if !CatalogV2Util.isSessionCatalog(catalog) =>
        Some(catalog, ident)
      case _ => None
    }
  }

  /**
   * Extract catalog and namespace from a multi-part name with the current catalog if needed.
   * Catalog name takes precedence over namespaces.
   */
  object CatalogAndNamespace {
    def unapply(nameParts: Seq[String]): Some[(CatalogPlugin, Seq[String])] = {
      assert(nameParts.nonEmpty)
      try {
        Some((catalogManager.catalog(nameParts.head), nameParts.tail))
      } catch {
        case _: CatalogNotFoundException =>
          Some((currentCatalog, nameParts))
      }
    }
  }

  /**
   * Extract catalog and identifier from a multi-part name with the current catalog if needed.
   * Catalog name takes precedence over identifier, but for a single-part name, identifier takes
   * precedence over catalog name.
   *
   * Note that, this pattern is used to look up permanent catalog objects like table, view,
   * function, etc. If you need to look up temp objects like temp view, please do it separately
   * before calling this pattern, as temp objects don't belong to any catalog.
   */
  object CatalogAndIdentifier {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.MultipartIdentifierHelper

    private val globalTempDB = SQLConf.get.getConf(StaticSQLConf.GLOBAL_TEMP_DATABASE)

    def unapply(nameParts: Seq[String]): Option[(CatalogPlugin, Identifier)] = {
      assert(nameParts.nonEmpty)
      if (nameParts.length == 1) {
        Some((currentCatalog, Identifier.of(catalogManager.currentNamespace, nameParts.head)))
      } else if (nameParts.head.equalsIgnoreCase(globalTempDB)) {
        // Conceptually global temp views are in a special reserved catalog. However, the v2 catalog
        // API does not support view yet, and we have to use v1 commands to deal with global temp
        // views. To simplify the implementation, we put global temp views in a special namespace
        // in the session catalog. The special namespace has higher priority during name resolution.
        // For example, if the name of a custom catalog is the same with `GLOBAL_TEMP_DATABASE`,
        // this custom catalog can't be accessed.
        Some((catalogManager.v2SessionCatalog, nameParts.asIdentifier))
      } else {
        try {
          Some((catalogManager.catalog(nameParts.head), nameParts.tail.asIdentifier))
        } catch {
          case _: CatalogNotFoundException =>
            Some((currentCatalog, nameParts.asIdentifier))
        }
      }
    }
  }

  /**
   * Extract legacy table identifier from a multi-part identifier.
   *
   * For legacy support only. Please use [[CatalogAndIdentifier]] instead on DSv2 code paths.
   */
  object AsTableIdentifier {
    def unapply(parts: Seq[String]): Option[TableIdentifier] = {
      def namesToTableIdentifier(names: Seq[String]): Option[TableIdentifier] = names match {
        case Seq(name) => Some(TableIdentifier(name))
        case Seq(database, name) => Some(TableIdentifier(name, Some(database)))
        case _ => None
      }
      parts match {
        case CatalogAndMultipartIdentifier(None, names)
          if CatalogV2Util.isSessionCatalog(currentCatalog) =>
          namesToTableIdentifier(names)
        case CatalogAndMultipartIdentifier(Some(catalog), names)
          if CatalogV2Util.isSessionCatalog(catalog) &&
             CatalogV2Util.isSessionCatalog(currentCatalog) =>
          namesToTableIdentifier(names)
        case _ => None
      }
    }
  }
}
