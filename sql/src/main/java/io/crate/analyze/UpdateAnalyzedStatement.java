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

import com.google.common.collect.ImmutableList;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Symbol;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.Path;
import io.crate.metadata.Reference;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class UpdateAnalyzedStatement implements AnalyzedRelation, AnalyzedStatement {

    private final List<NestedAnalyzedStatement> nestedStatements;
    private final AnalyzedRelation sourceRelation;


    public UpdateAnalyzedStatement(AnalyzedRelation sourceRelation, List<NestedAnalyzedStatement> nestedStatements) {
        this.sourceRelation = sourceRelation;
        this.nestedStatements = nestedStatements;
    }

    public AnalyzedRelation sourceRelation() {
        return sourceRelation;
    }

    public List<NestedAnalyzedStatement> nestedStatements() {
        return nestedStatements;
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> analyzedStatementVisitor, C context) {
        return analyzedStatementVisitor.visitUpdateStatement(this, context);
    }

    public static class NestedAnalyzedStatement {

        private final WhereClause whereClause;
        private final Map<Reference, Symbol> assignments = new HashMap<>();

        public NestedAnalyzedStatement(WhereClause whereClause) {
            this.whereClause = whereClause;
        }

        public Map<Reference, Symbol> assignments() {
            return assignments;
        }

        public WhereClause whereClause() {
            return whereClause;
        }

        public void addAssignment(Reference reference, Symbol value) {
            if (assignments.containsKey(reference)) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH, "reference repeated %s", reference.ident().columnIdent().sqlFqn()));
            }
            assignments.put(reference, value);
        }
    }

    @Override
    public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
        return visitor.visitUpdateAnalyzedStatement(this, context);
    }

    @Override
    public Field getField(Path path, Operation operation) throws UnsupportedOperationException, ColumnUnknownException {
        throw new UnsupportedOperationException("getField on UpdateAnalyzedStatement is not implemented");
    }

    @Override
    public List<Field> fields() {
        return ImmutableList.of();
    }

    @Override
    public boolean isWriteOperation() {
        return true;
    }

    @Override
    public QualifiedName getQualifiedName() {
        throw new UnsupportedOperationException("method not supported");
    }

    @Override
    public void setQualifiedName(@Nonnull QualifiedName qualifiedName) {
        throw new UnsupportedOperationException("method not supported");
    }
}
