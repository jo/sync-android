//  Copyright (c) 2014 Cloudant. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package com.cloudant.sync.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import com.cloudant.sync.datastore.DatastoreExtended;
import com.cloudant.sync.datastore.DatastoreManager;
import com.cloudant.sync.datastore.DocumentBodyFactory;
import com.cloudant.sync.datastore.MutableDocumentRevision;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.util.TestUtils;

import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 *  The purpose of this abstract class is to provide setup methods that include
 *  the loading of test data and the creation of indexes for use by the
 *  {@link QueryCoveringIndexesTest} class and the
 *  {@link QueryWithoutCoveringIndexesTest} class test methods.
 *
 *  @see QueryCoveringIndexesTest
 *  @see QueryWithoutCoveringIndexesTest
 */
public abstract class AbstractQueryTestBase {

    String factoryPath = null;
    DatastoreManager factory = null;
    DatastoreExtended ds = null;
    IndexManager im = null;
    SQLDatabase db = null;

    @Before
    public void setUp() throws Exception {
        factoryPath = TestUtils.createTempTestingDir(AbstractQueryTestBase.class.getName());
        assertThat(factoryPath, is(notNullValue()));
        factory = new DatastoreManager(factoryPath);
        assertThat(factory, is(notNullValue()));
        String datastoreName = AbstractQueryTestBase.class.getSimpleName();
        ds = (DatastoreExtended) factory.openDatastore(datastoreName);
        assertThat(ds, is(notNullValue()));
    }

    @After
    public void tearDown() {
        im.close();
        assertThat(im.getQueue().isShutdown(), is(true));
        TestUtils.deleteDatabaseQuietly(db);
        ds.close();
        TestUtils.deleteTempTestingDir(factoryPath);

        db = null;
        im = null;
        ds = null;
        factory = null;
        factoryPath = null;
    }

    // Used to setup document data testing:
    // - When executing AND queries
    // - When limiting and offsetting results
    // - When querying using _id
    // - When querying using _rev
    // - When querying using $not operator
    // - When querying using $exists operator
    public void setUpBasicQueryData() throws Exception {
        MutableDocumentRevision rev = new MutableDocumentRevision();
        rev.docId = "mike12";
        Map<String, Object> bodyMap = new HashMap<String, Object>();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 12);
        bodyMap.put("pet", "cat");
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);


        rev.docId = "mike34";
        bodyMap.clear();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 34);
        bodyMap.put("pet", "dog");
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);


        rev.docId = "mike72";
        bodyMap.clear();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 72);
        bodyMap.put("pet", "cat");
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);


        rev.docId = "fred34";
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 34);
        bodyMap.put("pet", "cat");
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);


        rev.docId = "fred12";
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 12);
        rev.body = DocumentBodyFactory.create(bodyMap); 
        ds.createDocumentFromRevision(rev);


        assertThat(im.ensureIndexed(Arrays.<Object>asList("name", "age"), "basic"), is("basic"));
        assertThat(im.ensureIndexed(Arrays.<Object>asList("name", "pet"), "pet"), is("pet"));
    }

    // Used to setup document data testing:
    // - When using dotted notation
    public void setUpDottedQueryData() throws Exception {
        setUpSharedDocs();

        assertThat(im.ensureIndexed(Arrays.<Object>asList("age", "pet.name", "pet.species"), "pet"),
                is("pet"));
        assertThat(im.ensureIndexed(Arrays.<Object>asList("name", "pet.name.first"), "firstname"),
                is("firstname"));
    }

    // Used to setup document data testing:
    // - When using non-ascii text
    public void setUpNonAsciiQueryData() throws Exception {
        MutableDocumentRevision rev = new MutableDocumentRevision();
        rev.docId = "mike12";
        Map<String, Object> bodyMap = new HashMap<String, Object>();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 12);
        bodyMap.put("pet", "cat");
        rev.body = DocumentBodyFactory.create(bodyMap);

        ds.createDocumentFromRevision(rev);


        rev.docId = "mike34";
        bodyMap.clear();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 34);
        bodyMap.put("pet", "dog");
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);


        rev.docId = "mike72";
        bodyMap.clear();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 72);
        bodyMap.put("pet", "cat");
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);


        rev.docId = "اسم34";
        bodyMap.clear();
        bodyMap.put("name", "اسم");
        bodyMap.put("age", 34);
        bodyMap.put("pet", "cat");
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);

        rev.docId = "fred12";
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 12);
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);

        rev.docId = "fredarabic";
        bodyMap.clear();
        bodyMap.put("اسم", "fred");
        bodyMap.put("age", 12);
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);


        rev.docId = "freddatatype";
        bodyMap.clear();
        bodyMap.put("datatype", "fred");
        bodyMap.put("age", 12);
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);

    }

    // Used to setup document data testing:
    // - When using OR queries
    public void setUpOrQueryData() throws Exception {
        setUpSharedDocs();

        assertThat(im.ensureIndexed(Arrays.<Object>asList("age", "pet", "name"), "basic"),
                is("basic"));
        assertThat(im.ensureIndexed(Arrays.<Object>asList("age", "pet.name", "pet.species"), "pet"),
                is("pet"));
        assertThat(im.ensureIndexed(Arrays.<Object>asList("age", "pet.name.first"), "firstname"),
                is("firstname"));
    }

    // Used to setup document data testing:
    // - When using nested queries
    public void setUpNestedQueryData() throws Exception {
        MutableDocumentRevision rev = new MutableDocumentRevision();
        rev.docId = "mike12";
        Map<String, Object> bodyMap = new HashMap<String, Object>();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 12);
        bodyMap.put("pet", "cat");
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);
        rev.docId = "mike23";
        bodyMap.clear();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 23);
        bodyMap.put("pet", "parrot");
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);

        rev.docId = "mike34";
        bodyMap.clear();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 34);
        bodyMap.put("pet", "dog");
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);
        rev.docId = "john34";
        bodyMap.clear();
        bodyMap.put("name", "john");
        bodyMap.put("age", 34);
        bodyMap.put("pet", "fish");
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);

        rev.docId = "fred43";
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 43);
        bodyMap.put("pet", "snake");
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);

        rev.docId = "fred12";
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 12);
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);

        assertThat(im.ensureIndexed(Arrays.<Object>asList("age", "pet", "name"), "basic"),
                is("basic"));
    }

    // Used to setup document data testing:
    // - When indexing array fields
    // - When querying using $in operator
    public void setUpArrayIndexingData() throws Exception {
        MutableDocumentRevision rev = new MutableDocumentRevision();
        rev.docId = "mike12";
        Map<String, Object> bodyMap = new HashMap<String, Object>();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 12);
        bodyMap.put("pet", Arrays.<Object>asList("cat", "dog"));
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);

        rev.docId = "fred34";
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 34);
        bodyMap.put("pet", "parrot");
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);

        rev.docId = "mike34";
        bodyMap.clear();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 34);
        bodyMap.put("pet", Arrays.<Object>asList("cat", "dog", "fish"));
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);

        rev.docId = "fred12";
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 12);
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);

        rev.docId = "john44";
        bodyMap.clear();
        bodyMap.put("name", "john");
        bodyMap.put("age", 44);
        bodyMap.put("pet", Arrays.<Object>asList("hamster", "snake"));
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);

        rev.docId = "john22";
        bodyMap.clear();
        bodyMap.put("name", "john");
        bodyMap.put("age", 22);
        bodyMap.put("pet", "cat");
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);

        assertThat(im.ensureIndexed(Arrays.<Object>asList("name", "pet", "age"), "pet"),
                is("pet"));
    }

    // Used to setup document data testing for queries with mathematical operations.
    // - When querying using $mod operator
    public void setUpNumericOperationsQueryData() throws Exception {
        MutableDocumentRevision rev = new MutableDocumentRevision();
        rev.docId = "mike31";
        Map<String, Object> bodyMap = new HashMap<String, Object>();
        bodyMap.put("name", "mike");
        bodyMap.put("score", 31);
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);

        rev.docId = "fred11";
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("score", 11);
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);

        rev.docId = "john15";
        bodyMap.clear();
        bodyMap.put("name", "john");
        bodyMap.put("score", 15);
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);

        rev.docId = "john-15";
        bodyMap.clear();
        bodyMap.put("name", "john");
        bodyMap.put("score", -15);
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);

        rev.docId = "john15.2";
        bodyMap.clear();
        bodyMap.put("name", "john");
        bodyMap.put("score", 15.2);
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);

        rev.docId = "john15.6";
        bodyMap.clear();
        bodyMap.put("name", "john");
        bodyMap.put("score", 15.6);
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);

        rev.docId = "john0";
        bodyMap.clear();
        bodyMap.put("name", "john");
        bodyMap.put("score", 0);
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);

        rev.docId = "john0.0";
        bodyMap.clear();
        bodyMap.put("name", "john");
        bodyMap.put("score", 0.0);
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);

        rev.docId = "john0.6";
        bodyMap.clear();
        bodyMap.put("name", "john");
        bodyMap.put("score", 0.6);
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);

        rev.docId = "john-0.6";
        bodyMap.clear();
        bodyMap.put("name", "john");
        bodyMap.put("score", -0.6);
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);

        assertThat(im.ensureIndexed(Arrays.<Object>asList("name", "score"), "name_score"),
                                    is("name_score"));
    }

    // Used to setup document data testing:
    // - When there is a large result set
    public void setUpLargeResultSetQueryData() throws Exception {
        MutableDocumentRevision rev = new MutableDocumentRevision();
        for (int i = 0; i < 150; i++) {
            rev.docId = String.format("d%d", i);
            Map<String, Object> bodyMap = new HashMap<String, Object>();
            bodyMap.put("large_field", "cat");
            bodyMap.put("idx", i);
            rev.body = DocumentBodyFactory.create(bodyMap);
            ds.createDocumentFromRevision(rev);
        }
        assertThat(im.ensureIndexed(Arrays.<Object>asList("large_field", "idx"), "large"),
                is("large"));
    }

    // Used to setup document data testing for queries without covering indexes:
    // - When executing AND queries
    // - When executing OR queries
    public void setUpWithoutCoveringIndexesQueryData() throws Exception {
        MutableDocumentRevision rev = new MutableDocumentRevision();
        rev.docId = "mike12";
        Map<String, Object> bodyMap = new HashMap<String, Object>();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 12);
        bodyMap.put("pet", "cat");
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);

        rev.docId = "mike34";
        bodyMap.clear();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 34);
        bodyMap.put("pet", "dog");
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);

        rev.docId = "mike72";
        bodyMap.clear();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 72);
        bodyMap.put("pet", "cat");
        bodyMap.put("town", "bristol");
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);

        rev.docId = "fred34";
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 34);
        bodyMap.put("pet", "cat");
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);

        rev.docId = "fred12";
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 12);
        bodyMap.put("town", "bristol");
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);

        assertThat(im.ensureIndexed(Arrays.<Object>asList("name", "age"), "basic"), is("basic"));
        assertThat(im.ensureIndexed(Arrays.<Object>asList("name", "pet"), "pet"), is("pet"));
    }

    // Used to setup document data testing for queries containing a $size operator:
    // - When executing queries containing $size operator
    public void setUpSizeOperatorQueryData() throws Exception {
        MutableDocumentRevision rev = new MutableDocumentRevision();
        rev.docId = "mike24";
        Map<String, Object> bodyMap = new HashMap<String, Object>();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 24);
        bodyMap.put("pet", Collections.singletonList("cat"));
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);

        rev.docId = "mike12";
        bodyMap.clear();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 12);
        bodyMap.put("pet", Arrays.asList("cat", "dog"));
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);

        rev.docId = "fred34";
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 34);
        bodyMap.put("pet", Arrays.asList("cat", "dog"));
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);

        rev.docId = "john44";
        bodyMap.clear();
        bodyMap.put("name", "john");
        bodyMap.put("age", 44);
        bodyMap.put("pet", "cat");
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);

        rev.docId = "fred72";
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 72);
        bodyMap.put("pet", Collections.singletonList("dog"));
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);

        rev.docId = "john12";
        bodyMap.clear();
        bodyMap.put("name", "john");
        bodyMap.put("age", 12);
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);

        rev.docId = "bill34";
        bodyMap.clear();
        bodyMap.put("name", "bill");
        bodyMap.put("age", 34);
        bodyMap.put("pet", Arrays.asList("cat", "parrot"));
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);

        rev.docId = "fred11";
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 11);
        bodyMap.put("pet", new ArrayList<Object>());
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);

        assertThat(im.ensureIndexed(Arrays.<Object>asList("name", "pet", "age"), "basic"), is("basic"));
    }

    // Used to setup document data testing for sorting:
    // - When sorting
    public void setUpSortingQueryData() throws Exception {
        MutableDocumentRevision rev = new MutableDocumentRevision();
        rev.docId = "mike12";
        Map<String, Object> bodyMap = new HashMap<String, Object>();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 12);
        bodyMap.put("age", Arrays.<Object>asList("cat", "dog"));
        bodyMap.put("same", "all");
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);

        rev.docId = "fred34";
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 34);
        bodyMap.put("pet", "parrot");
        bodyMap.put("same", "all");
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);

        rev.docId = "fred11";
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 11);
        bodyMap.put("pet", "fish");
        bodyMap.put("same", "all");
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);

        assertThat(im.ensureIndexed(Arrays.<Object>asList("name", "pet", "age", "same"), "pet"),
                is("pet"));
    }

    private void setUpSharedDocs() throws Exception {
        MutableDocumentRevision rev = new MutableDocumentRevision();
        rev.docId = "mike12";
        Map<String, Object> bodyMap = new HashMap<String, Object>();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 12);
        Map<String, Object> petMap = new HashMap<String, Object>();
        petMap.put("species", "cat");
        petMap.put("name", "mike");
        bodyMap.put("pet", petMap);
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);

        rev.docId = "mike23";
        bodyMap.clear();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 23);
        petMap.clear();
        petMap.put("species", "cat");
        Map<String, Object> petNameMap = new HashMap<String, Object>();
        petNameMap.put("first", "mike");
        petMap.put("name", petNameMap);
        bodyMap.put("pet", petMap);
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);

        rev.docId = "mike34";
        bodyMap.clear();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 34);
        petMap.clear();
        petMap.put("species", "cat");
        petMap.put("name", "mike");
        bodyMap.put("pet", petMap);
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);

        rev.docId = "mike72";
        bodyMap.clear();
        bodyMap.put("name", "mike");
        bodyMap.put("age", 72);
        bodyMap.put("pet", "cat");
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);

        rev.docId = "fred34";
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 34);
        bodyMap.put("pet", "cat");
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);

        rev.docId = "fred12";
        bodyMap.clear();
        bodyMap.put("name", "fred");
        bodyMap.put("age", 12);
        rev.body = DocumentBodyFactory.create(bodyMap);
        ds.createDocumentFromRevision(rev);
    }

}
