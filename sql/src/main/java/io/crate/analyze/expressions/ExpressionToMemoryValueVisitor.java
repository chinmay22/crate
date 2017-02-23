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

package io.crate.analyze.expressions;

import io.crate.data.Row;
import io.crate.sql.tree.*;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.MemorySizeValue;

import java.util.Locale;

public class ExpressionToMemoryValueVisitor {

    private static final Visitor VISITOR = new Visitor();

    private ExpressionToMemoryValueVisitor() {
    }

    public static ByteSizeValue convert(Node node, Row parameters, String settingName) {
        return VISITOR.process(node, new Context(settingName, parameters));
    }

    private static class Context {
        private final String settingName;
        private final Row params;

        public Context(String settingName, Row params) {
            this.settingName = settingName;
            this.params = params;
        }
    }

    private static class Visitor extends AstVisitor<ByteSizeValue, Context> {

        @Override
        protected ByteSizeValue visitStringLiteral(StringLiteral node, Context context) {
            try {
                return MemorySizeValue.parseBytesSizeValueOrHeapRatio(node.getValue(), context.settingName);
            } catch (ElasticsearchParseException e) {
                throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "Invalid byte size value '%s'", node.getValue()));
            }
        }

        @Override
        protected ByteSizeValue visitLongLiteral(LongLiteral node, Context context) {
            return new ByteSizeValue(node.getValue());
        }

        @Override
        public ByteSizeValue visitParameterExpression(ParameterExpression node, Context context) {
            ByteSizeValue value;
            Object param = context.params.get(node.index());
            if (param instanceof Number) {
                value = new ByteSizeValue(((Number) param).longValue());
            } else if (param instanceof String) {
                try {
                    value = MemorySizeValue.parseBytesSizeValueOrHeapRatio((String) param, context.settingName);
                } catch (ElasticsearchParseException e) {
                    throw new IllegalArgumentException(
                        String.format(Locale.ENGLISH, "Invalid byte size value '%s'", param));
                }
            } else {
                throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "Invalid byte size value %s", param));
            }
            return value;
        }

        @Override
        protected ByteSizeValue visitNode(Node node, Context context) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH, "Invalid byte size value %s", node));
        }
    }
}
