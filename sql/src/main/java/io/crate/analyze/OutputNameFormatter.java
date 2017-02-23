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

import com.google.common.collect.Iterables;
import io.crate.sql.ExpressionFormatter;
import io.crate.sql.tree.ArrayComparisonExpression;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.QualifiedNameReference;
import io.crate.sql.tree.SubscriptExpression;

import java.util.ArrayList;
import java.util.List;

public class OutputNameFormatter {

    private final static InnerOutputNameFormatter INSTANCE = new InnerOutputNameFormatter();

    public static String format(Expression expression) {
        return INSTANCE.process(expression, null);
    }

    private static class InnerOutputNameFormatter extends ExpressionFormatter.Formatter {
        @Override
        protected String visitQualifiedNameReference(QualifiedNameReference node, Void context) {

            List<String> parts = new ArrayList<>();
            for (String part : node.getName().getParts()) {
                parts.add(part);
            }
            return Iterables.getLast(parts);
        }

        @Override
        protected String visitSubscriptExpression(SubscriptExpression node, Void context) {
            return process(node.name(), null) + '[' + process(node.index(), null) + ']';
        }

        @Override
        public String visitArrayComparisonExpression(ArrayComparisonExpression node, Void context) {
            return process(node.getLeft(), null) + ' ' +
                   node.getType().getValue() + ' ' +
                   node.quantifier().name() + '(' +
                   process(node.getRight(), null) + ')';
        }
    }
}
