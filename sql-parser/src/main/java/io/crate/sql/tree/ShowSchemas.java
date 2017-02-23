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

import java.util.Objects;
import java.util.Optional;

public class ShowSchemas extends Statement {

    private final Optional<String> likePattern;
    private final Optional<Expression> whereExpression;

    public ShowSchemas(Optional<String> likePattern, Optional<Expression> whereExpr) {
        this.likePattern = likePattern;
        this.whereExpression = whereExpr;
    }

    public Optional<String> likePattern() {
        return likePattern;
    }

    public Optional<Expression> whereExpression() {
        return whereExpression;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowSchemas(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShowSchemas that = (ShowSchemas) o;
        return Objects.equals(likePattern, that.likePattern) &&
            Objects.equals(whereExpression, that.whereExpression);
    }

    @Override
    public int hashCode() {
        return Objects.hash(likePattern, whereExpression);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("likePattern", likePattern)
            .add("whereExpression", whereExpression)
            .toString();
    }

}
