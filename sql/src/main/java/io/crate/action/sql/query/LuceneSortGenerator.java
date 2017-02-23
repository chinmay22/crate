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

package io.crate.action.sql.query;

import io.crate.analyze.OrderBy;
import io.crate.lucene.FieldTypeLookup;
import io.crate.operation.collect.DocInputFactory;
import io.crate.operation.reference.doc.lucene.CollectorContext;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;

import javax.annotation.Nullable;

public class LuceneSortGenerator {

    @Nullable
    public static Sort generateLuceneSort(CollectorContext context,
                                          OrderBy orderBy,
                                          DocInputFactory docInputFactory,
                                          FieldTypeLookup fieldTypeLookup) {
        if (orderBy.orderBySymbols().isEmpty()) {
            return null;
        }
        SortSymbolVisitor sortSymbolVisitor = new SortSymbolVisitor(docInputFactory, fieldTypeLookup);
        SortField[] sortFields = sortSymbolVisitor.generateSortFields(
            orderBy.orderBySymbols(),
            context,
            orderBy.reverseFlags(),
            orderBy.nullsFirst()
        );
        return new Sort(sortFields);
    }
}
