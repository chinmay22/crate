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

package io.crate.analyze;

import io.crate.data.Row;
import io.crate.sql.tree.AstVisitor;
import io.crate.sql.tree.Node;
import io.crate.sql.tree.ParameterExpression;
import io.crate.sql.tree.StringLiteral;
import org.apache.lucene.util.BytesRef;

import javax.annotation.Nullable;
import java.util.Locale;

public class SafeExpressionToStringVisitor extends AstVisitor<String, Row> {

    private final static SafeExpressionToStringVisitor INSTANCE = new SafeExpressionToStringVisitor();

    private SafeExpressionToStringVisitor() {
    }

    public static String convert(Node node, @Nullable Row context) {
        return INSTANCE.process(node, context);
    }

    @Override
    protected String visitStringLiteral(StringLiteral node, Row parameters) {
        return node.getValue();
    }

    @Override
    public String visitParameterExpression(ParameterExpression node, Row parameters) {
        Object value = parameters.get(node.index());
        if (value instanceof BytesRef) {
            return ((BytesRef) value).utf8ToString();
        }
        if (!(value instanceof String)) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH, "Parameter %s not a string value, can't handle this.", value));
        }
        return (String) value;
    }

    @Override
    protected String visitNode(Node node, Row context) {
        throw new IllegalArgumentException(String.format(Locale.ENGLISH, "Can't handle %s.", node));
    }
}
