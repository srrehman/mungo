/**
 * 	
 * Copyright 2013 Pagecrumb
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *  
 */
package com.mungods;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.bson.types.ObjectId;
import org.json.simple.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.inject.Inject;
import com.mungods.Mungo;
import com.mungods.collection.WriteResult;
import com.mungods.object.GAEObject;
import com.mungods.object.GAEObjectFactory;
import com.mungods.shell.Connector;

public class XObjectTest {
	
    private final LocalServiceTestHelper helper =
            new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig()
                .setDefaultHighRepJobPolicyUnappliedJobPercentage(0)); 	   

    @Inject
    private GAEObjectFactory xf;
    
    @Before
    public void setUp() {
        helper.setUp(); 
        xf = new Connector().getFactory();
    }

    @After
    public void tearDown() {
        helper.tearDown();
    }	
    
    @Test(expected = IllegalArgumentException.class)
    public void testExecuteNullCmd() {
    	GAEObject xobj = xf.get("testDB", "testCollection");
    	xobj.execute();
    }
 
    @Test
    public void test() {
    	GAEObject xobj = xf.get("testDB", "testCollection");
    	xobj.setCommand("$insert");
    	xobj.execute();
    }
	
}

