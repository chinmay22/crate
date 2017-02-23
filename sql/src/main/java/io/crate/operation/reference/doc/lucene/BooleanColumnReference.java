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

package io.crate.operation.reference.doc.lucene;

import io.crate.exceptions.GroupByOnArrayUnsupportedException;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;

import java.io.IOException;

public class BooleanColumnReference extends LuceneCollectorExpression<Boolean> {

    private static final BytesRef TRUE_BYTESREF = new BytesRef("1");
    private SortedBinaryDocValues values;
    private Boolean value;

    public BooleanColumnReference(String columnName) {
        super(columnName);
    }

    @Override
    public Boolean value() {
        return value;
    }

    @Override
    public void setNextDocId(int docId) {
        super.setNextDocId(docId);
        values.setDocument(docId);
        switch (values.count()) {
            case 0:
                value = null;
                break;
            case 1:
                value = values.valueAt(0).compareTo(TRUE_BYTESREF) == 0;
                break;
            default:
                throw new GroupByOnArrayUnsupportedException(columnName);
        }
    }

    @Override
    public void setNextReader(LeafReaderContext context) throws IOException {
        super.setNextReader(context);
        values = FieldData.toString(DocValues.getSortedNumeric(context.reader(), columnName));
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (obj == this)
            return true;
        if (!(obj instanceof BooleanColumnReference))
            return false;
        return columnName.equals(((BooleanColumnReference) obj).columnName);
    }

    @Override
    public int hashCode() {
        return columnName.hashCode();
    }
}
