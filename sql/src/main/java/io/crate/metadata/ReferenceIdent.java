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

package io.crate.metadata;

import com.google.common.base.Objects;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Locale;

public class ReferenceIdent {

    private final TableIdent tableIdent;
    private final ColumnIdent columnIdent;

    public ReferenceIdent(StreamInput in) throws IOException {
        columnIdent = new ColumnIdent(in);
        tableIdent = new TableIdent(in);
    }

    public ReferenceIdent(TableIdent tableIdent, ColumnIdent columnIdent) {
        this.tableIdent = tableIdent;
        this.columnIdent = columnIdent;
    }

    public ReferenceIdent(TableIdent tableIdent, String column) {
        this(tableIdent, new ColumnIdent(column));
    }

    public ReferenceIdent(TableIdent tableIdent, String column, @Nullable List<String> path) {
        this(tableIdent, new ColumnIdent(column, path));
    }

    public TableIdent tableIdent() {
        return tableIdent;
    }

    public boolean isColumn() {
        return columnIdent.isColumn();
    }

    public ColumnIdent columnIdent() {
        return columnIdent;
    }

    public ReferenceIdent columnReferenceIdent() {
        if (isColumn()) {
            return this;
        }
        return new ReferenceIdent(tableIdent, columnIdent.name());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        ReferenceIdent o = (ReferenceIdent) obj;
        return Objects.equal(columnIdent, o.columnIdent) &&
               Objects.equal(tableIdent, o.tableIdent);
    }

    @Override
    public int hashCode() {
        int result = tableIdent.hashCode();
        result = 31 * result + columnIdent.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return String.format(Locale.ENGLISH, "<RefIdent: %s->%s>", tableIdent, columnIdent);
    }

    public void writeTo(StreamOutput out) throws IOException {
        columnIdent.writeTo(out);
        tableIdent.writeTo(out);
    }
}
