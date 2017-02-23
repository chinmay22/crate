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

package io.crate.executor.transport.task.elasticsearch;

import com.google.common.base.Function;
import io.crate.metadata.ColumnIdent;
import io.crate.types.DataType;
import org.elasticsearch.search.SearchHit;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class ESFieldExtractor implements Function<SearchHit, Object> {

    private static final Object NOT_FOUND = new Object();

    public static class Source extends ESFieldExtractor {

        private final ColumnIdent ident;
        private final DataType type;

        public Source(ColumnIdent ident, DataType type) {
            this.ident = ident;
            this.type = type;
        }

        @Override
        public Object apply(SearchHit hit) {
            return toValue(hit.getSource());
        }

        Object down(Object c, int idx) {
            if (idx == ident.path().size()) {
                return c == NOT_FOUND ? null : c;
            }
            if (c instanceof List) {
                List l = (List) c;
                List<Object> children = new ArrayList<>(l.size());
                for (Object child : l) {
                    Object sub = down(child, idx);
                    if (sub != NOT_FOUND) {
                        children.add(sub);
                    }
                }
                return children;
            } else if (c instanceof Map) {
                Map cm = ((Map) c);
                if (cm.containsKey(ident.path().get(idx))) {
                    return down(((Map) c).get(ident.path().get(idx)), idx + 1);
                } else {
                    return NOT_FOUND;
                }
            }
            throw new IndexOutOfBoundsException("Failed to get path");
        }

        /**
         * This method extracts data for the given column ident, by traversing the source map.
         * If more than one object matches, the result is a list of the matching values, otherwise a single object.
         *
         * @param source the source as a map
         * @return the value calculated for the columnIdent
         */
        Object toValue(@Nullable Map<String, Object> source) {
            if (source == null || source.size() == 0) {
                return null;
            }
            Object top = source.get(ident.name());
            if (ident.isColumn()) {
                return type.value(top);
            }
            if (top == null) {
                return null;
            }
            Object result = down(top, 0);
            return result == NOT_FOUND ? null : type.value(result);
        }
    }
}
