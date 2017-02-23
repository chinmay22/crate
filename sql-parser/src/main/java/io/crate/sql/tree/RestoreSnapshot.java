/*
 * Licensed to Crate.io Inc. (Crate) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file to
 * you under the Apache License, Version 2.0 (the "License");  you may not
 * use this file except in compliance with the License.  You may obtain a
 * copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, to use any modules in this file marked as "Enterprise Features",
 * Crate must have given you permission to enable and use such Enterprise
 * Features and you must have a valid Enterprise or Subscription Agreement
 * with Crate.  If you enable or use the Enterprise Features, you represent
 * and warrant that you have a valid Enterprise or Subscription Agreement
 * with Crate.  Your use of the Enterprise Features if governed by the terms
 * and conditions of your Enterprise or Subscription Agreement with Crate.
 */

package io.crate.sql.tree;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import java.util.List;
import java.util.Optional;

public class RestoreSnapshot extends Statement {

    private final QualifiedName name;
    private final Optional<GenericProperties> properties;
    private final Optional<List<Table>> tableList;

    public RestoreSnapshot(QualifiedName name, Optional<GenericProperties> genericProperties) {
        this.name = name;
        this.properties = genericProperties;
        this.tableList = Optional.empty();

    }

    public RestoreSnapshot(QualifiedName name,
                           List<Table> tableList,
                           Optional<GenericProperties> genericProperties) {
        this.name = name;
        this.tableList = Optional.of(tableList);
        this.properties = genericProperties;

    }

    public QualifiedName name() {
        return this.name;
    }

    public Optional<GenericProperties> properties() {
        return properties;
    }

    public Optional<List<Table>> tableList() {
        return tableList;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name, tableList, properties);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        RestoreSnapshot that = (RestoreSnapshot) obj;
        if (!name.equals(that.name)) return false;
        if (!properties.equals(that.properties)) return false;
        if (!tableList.equals(that.tableList)) return false;
        return true;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("name", name)
            .add("properties", properties)
            .add("tableList", tableList)
            .toString();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitRestoreSnapshot(this, context);
    }
}
