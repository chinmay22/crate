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

import io.crate.metadata.doc.DocSysColumns;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class ColumnIdentTest {

    @Test
    public void testSqlFqn() throws Exception {
        ColumnIdent ident = new ColumnIdent("foo", Arrays.asList("x", "y", "z"));
        assertThat(ident.sqlFqn(), is("foo['x']['y']['z']"));

        ident = new ColumnIdent("a");
        assertThat(ident.sqlFqn(), is("a"));

        ident = new ColumnIdent("a", Collections.singletonList(""));
        assertThat(ident.sqlFqn(), is("a['']"));

        ident = new ColumnIdent("a.b", Collections.singletonList("c"));
        assertThat(ident.sqlFqn(), is("a.b['c']"));
    }

    @Test
    public void testShiftRight() throws Exception {
        assertThat(new ColumnIdent("foo", "bar").shiftRight(), is(new ColumnIdent("bar")));
        assertThat(new ColumnIdent("foo", Arrays.asList("x", "y", "z")).shiftRight(),
            is(new ColumnIdent("x", Arrays.asList("y", "z"))));
        assertThat(new ColumnIdent("foo").shiftRight(), Matchers.nullValue());
    }

    @Test
    public void testIsChildOf() throws Exception {
        ColumnIdent root = new ColumnIdent("root");
        ColumnIdent rootX = new ColumnIdent("root", "x");
        ColumnIdent rootXY = new ColumnIdent("root", Arrays.asList("x", "y"));
        ColumnIdent rootYX = new ColumnIdent("root", Arrays.asList("y", "x"));

        assertThat(root.isChildOf(root), is(false));

        assertThat(rootX.isChildOf(root), is(true));
        assertThat(rootXY.isChildOf(root), is(true));
        assertThat(rootXY.isChildOf(rootX), is(true));

        assertThat(rootYX.isChildOf(root), is(true));
        assertThat(rootYX.isChildOf(rootX), is(false));
    }

    @Test
    public void testPrepend() throws Exception {
        ColumnIdent foo = new ColumnIdent("foo");
        assertThat(foo.prepend(DocSysColumns.DOC.name()),
            is(new ColumnIdent(DocSysColumns.DOC.name(), "foo")));

        ColumnIdent fooBar = new ColumnIdent("foo", "bar");
        assertThat(fooBar.prepend("x"), is(new ColumnIdent("x", Arrays.asList("foo", "bar"))));
    }
}
