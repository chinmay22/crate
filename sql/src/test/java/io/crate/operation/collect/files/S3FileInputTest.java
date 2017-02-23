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

package io.crate.operation.collect.files;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Predicate;
import io.crate.external.S3ClientHelper;
import io.crate.test.integration.CrateUnitTest;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URI;
import java.util.LinkedList;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class S3FileInputTest extends CrateUnitTest {

    private static S3FileInput s3FileInput;
    private static List<S3ObjectSummary> listObjectSummaries;

    private static ObjectListing objectListing = mock(ObjectListing.class);
    private static S3ClientHelper clientBuilder = mock(S3ClientHelper.class);
    private static AmazonS3 amazonS3 = mock(AmazonS3.class);
    private static Predicate<URI> uriPredicate = mock(Predicate.class);

    private static final String BUCKET_NAME = "fakeBucket";
    private static final String PREFIX = "prefix";
    private static URI uri;


    @BeforeClass
    public static void setUpClass() throws Exception {
        uri = new URI("s3://fakeBucket/prefix");
        s3FileInput = new S3FileInput(clientBuilder);

        when(uriPredicate.apply(any(URI.class))).thenReturn(true);
        when(amazonS3.listObjects(BUCKET_NAME, PREFIX)).thenReturn(objectListing);
        when(clientBuilder.client(uri)).thenReturn(amazonS3);
    }

    @Test
    public void testListListUrlsWhenEmptyKeysIsListed() throws Exception {
        S3ObjectSummary path = new S3ObjectSummary();
        path.setBucketName(BUCKET_NAME);
        path.setKey("prefix/");
        listObjectSummaries = objectSummaries();
        listObjectSummaries.add(path);

        when(objectListing.getObjectSummaries()).thenReturn(listObjectSummaries);

        List<URI> uris = s3FileInput.listUris(uri, uriPredicate);
        assertThat(uris.size(), is(2));
        assertThat(uris.get(0).toString(), is("s3://fakeBucket/prefix/test1.json.gz"));
        assertThat(uris.get(1).toString(), is("s3://fakeBucket/prefix/test2.json.gz"));
    }

    @Test
    public void testListListUrlsWithCorrectKeys() throws Exception {
        when(objectListing.getObjectSummaries()).thenReturn(objectSummaries());

        List<URI> uris = s3FileInput.listUris(uri, uriPredicate);
        assertThat(uris.size(), is(2));
        assertThat(uris.get(0).toString(), is("s3://fakeBucket/prefix/test1.json.gz"));
        assertThat(uris.get(1).toString(), is("s3://fakeBucket/prefix/test2.json.gz"));
    }

    private List<S3ObjectSummary> objectSummaries() {
        listObjectSummaries = new LinkedList<>();

        S3ObjectSummary firstObj = new S3ObjectSummary();
        S3ObjectSummary secondObj = new S3ObjectSummary();
        firstObj.setBucketName(BUCKET_NAME);
        secondObj.setBucketName(BUCKET_NAME);
        firstObj.setKey("prefix/test1.json.gz");
        secondObj.setKey("prefix/test2.json.gz");
        listObjectSummaries.add(firstObj);
        listObjectSummaries.add(secondObj);
        return listObjectSummaries;
    }

}
