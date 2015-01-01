/*
 * Copyright (c) 2006-2011 Nuxeo SA (http://nuxeo.com/) and others.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 */
package org.nuxeo.ecm.core.opencmis.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.inject.Inject;
import org.apache.chemistry.opencmis.commons.data.ObjectData;
import org.apache.chemistry.opencmis.commons.data.Properties;
import org.apache.chemistry.opencmis.commons.data.PropertyData;
import org.apache.chemistry.opencmis.commons.data.PropertyString;
import org.apache.chemistry.opencmis.commons.spi.BindingsObjectFactory;
import org.apache.chemistry.opencmis.commons.spi.Holder;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.nuxeo.ecm.automation.core.util.DateTimeFormat;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.core.api.IdRef;
import org.nuxeo.ecm.core.api.impl.blob.ByteArrayBlob;
import org.nuxeo.ecm.core.opencmis.impl.server.NuxeoTypeHelper;
import org.nuxeo.ecm.core.schema.utils.DateParser;
import org.nuxeo.ecm.core.storage.sql.ra.PoolingRepositoryFactory;
import org.nuxeo.ecm.core.test.annotations.Granularity;
import org.nuxeo.ecm.core.test.annotations.RepositoryConfig;
import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.test.runner.Deploy;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;
import org.nuxeo.runtime.test.runner.LocalDeploy;

@RunWith(FeaturesRunner.class)
@Features({ CmisFeature.class, CmisFeatureConfiguration.class })
@Deploy({ "org.nuxeo.ecm.webengine.core", //
        "org.nuxeo.ecm.automation.core" //
})
@LocalDeploy("org.nuxeo.ecm.core.opencmis.tests.tests:OSGI-INF/types-contrib.xml")
@RepositoryConfig(cleanup = Granularity.METHOD, repositoryFactoryClass = PoolingRepositoryFactory.class)
public class TestCmisBindingComplexProperties extends TestCmisBindingBase {

    @Inject
    protected CoreSession coreSession;

    @Before
    public void setUp() throws Exception {
        setUpBinding(coreSession);
        setUpData(coreSession);
    }

    @After
    public void tearDown() {
        tearDownBinding();
    }

    protected ObjectData getObjectByPath(String path) {
        return objService.getObjectByPath(repositoryId, path, null, null, null, null, null, null, null);
    }

    protected Properties createProperties(String key, String value) {
        BindingsObjectFactory factory = binding.getObjectFactory();
        PropertyString prop = factory.createPropertyStringData(key, value);
        return factory.createPropertiesData(Collections.<PropertyData<?>>singletonList(prop));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGetComplexListProperty() throws Exception {
        //Enable complex properties
        Framework.getProperties().setProperty(NuxeoTypeHelper.ENABLE_COMPLEX_PROPERTIES, "true");

        //Create a complex property to encode
        ArrayList<HashMap> propList = createComplexPropertyList(3);

        //Set the property value on a document
        CoreSession session = coreSession;
        DocumentModel doc = session.createDocumentModel("/",null,"ComplexFile");
        doc.setPropertyValue("complexTest:listItem", propList);
        doc = session.createDocument(doc);
        session.save();
        doc.refresh();
        assertTrue(session.exists(new IdRef(doc.getId())));

        //Get the property as CMIS will see it from the object service
        Properties p = objService.getProperties(repositoryId, doc.getId(), null, null);
        assertNotNull(p);
        List<Object> cmisValues = (List<Object>) 
                p.getProperties().get("complexTest:listItem").getValues();
        assertEquals("Wrong number of marshaled values", propList.size(), cmisValues.size());

        //Verify the JSON produced is valid and matches the original objects
        ObjectMapper mapper = new ObjectMapper();
        for (int i = 0; i < cmisValues.size(); i++) {
            JsonNode jsonNode = mapper.readTree(cmisValues.get(i).toString());
            Map<String, Object> orig = propList.get(i);
            assertComplexPropertyNodeEquals(orig, jsonNode, DateTimeFormat.W3C);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSetComplexListProperty() throws Exception {
        //Enable complex properties
        Framework.getProperties().setProperty(NuxeoTypeHelper.ENABLE_COMPLEX_PROPERTIES, "true");

        //Create some JSON to pass into the CMIS service
        ArrayList<ObjectNode> nodeList = createComplexNodeList(3, DateTimeFormat.TIME_IN_MILLIS);

        //Get a document with the right property schema
        CoreSession session = coreSession;
        DocumentModel doc = session.createDocumentModel("/", null, "ComplexFile");
        doc = session.createDocument(doc);
        session.save();
    
        //Set the property as a JSON string through the CMIS service
        BindingsObjectFactory bof = binding.getObjectFactory();
        ArrayList<String> stringArr = new ArrayList<String>();
        for (int i = 0; i < nodeList.size(); i++) {
            stringArr.add(nodeList.get(i).toString());
        }
        PropertyString prop = bof.createPropertyStringData("complexTest:listItem", stringArr);
        Properties props = bof.createPropertiesData(
                Collections.<PropertyData<?>> singletonList(prop));
        Holder<String> objectIdHolder = new Holder<String>(doc.getId());
        objService.updateProperties(repositoryId, objectIdHolder, null, props, null);

        //Verify the properties produced in Nuxeo match the input JSON
        session.save();
        doc.refresh();
        List<Object> list = (List<Object>) doc.getPropertyValue("complexTest:listItem");
        assertEquals("Wrong number of elements in list", nodeList.size(), list.size());
        for (int i = 1; i < list.size(); i++) {
            JsonNode orig = nodeList.get(i);
            Map<String, Object> obj = (Map<String, Object>) list.get(i);
            assertComplexPropertyNodeEquals(obj, orig, DateTimeFormat.TIME_IN_MILLIS);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGetComplexProperty() throws Exception {
        //Enable complex properties
        Framework.getProperties().setProperty(NuxeoTypeHelper.ENABLE_COMPLEX_PROPERTIES, "true");

        //Create a complex property to encode
        ArrayList<HashMap> list = createComplexPropertyList(1);
        HashMap<String, Object> propMap = list.get(0);

        //Set the property value on a document
        CoreSession session = coreSession;
        DocumentModel doc = session.createDocumentModel("/", null, "ComplexFile");
        doc.setPropertyValue("complexTest:complexItem", propMap);
        ByteArrayBlob blob = new ByteArrayBlob("Test content".getBytes("UTF-8"), 
                "text/plain", "UTF-8", "test.txt", null);
        doc.setProperty("file", "content", blob);
        doc = session.createDocument(doc);
        session.save();
        doc.refresh();
        assertTrue(session.exists(new IdRef(doc.getId())));

        //Get the property as CMIS will see it from the object service
        Properties p = objService.getProperties(repositoryId, doc.getId(), null, null);
        assertNotNull(p);
        String jsonStr = p.getProperties().get(
                "complexTest:complexItem").getFirstValue().toString();
        assertEquals("Complex item should get marshaled as a single string value", 1,
                p.getProperties().get("complexTest:complexItem").getValues().size());
          
        //Verify the JSON produced is valid and matches the original objects
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = mapper.readTree(jsonStr);
        assertComplexPropertyNodeEquals(propMap, jsonNode, DateTimeFormat.W3C);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSetComplexProperty() throws Exception {
        //Enable complex properties
        Framework.getProperties().setProperty(NuxeoTypeHelper.ENABLE_COMPLEX_PROPERTIES, "true");

        //Create some JSON to pass into the CMIS service
        ArrayList<ObjectNode> nodeList = createComplexNodeList(1, DateTimeFormat.TIME_IN_MILLIS);
        ObjectNode jsonObj = nodeList.get(0);
        String jsonStr = jsonObj.toString();

        //Get a document with the right property schema
        CoreSession session = coreSession;
        DocumentModel doc = session.createDocumentModel("/", null, "ComplexFile");
        doc = session.createDocument(doc);
        session.save();
                
        //Set the property as a JSON string through the CMIS service
        Properties props = createProperties("complexTest:complexItem", jsonStr);
        Holder<String> objectIdHolder = new Holder<String>(doc.getId());
        objService.updateProperties(repositoryId, objectIdHolder, null, props, null);

        //Verify the properties produced in Nuxeo match the input JSON
        session.save();
        doc.refresh();
        Map<String, Object> propMap = (Map<String, Object>)
                doc.getPropertyValue("complexTest:complexItem");
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = mapper.readTree(jsonStr);
        assertComplexPropertyNodeEquals(propMap, jsonNode, DateTimeFormat.TIME_IN_MILLIS);
    }

    /**
     * Test that complex types are not exposed unless the enabled property is set
     */
    public void testEnableComplexProperties() throws Exception {
        //Don't enable complex properties for this test

        //Set a complex property on a document
        HashMap<String, Object> propMap = new HashMap<String, Object>();
        propMap.put("stringProp", "testString");
        
        //Set the property value on a document
        CoreSession session = coreSession;
        DocumentModel doc = session.createDocumentModel("/", null, "ComplexFile");
        doc.setPropertyValue("complexTest:complexItem", propMap);
        doc = session.createDocument(doc);
        session.save();
        doc.refresh();
        assertTrue(session.exists(new IdRef(doc.getId())));

        //Get the property as CMIS will see it from the object service
        Properties p = objService.getProperties(repositoryId, doc.getId(), null, null);
        assertNotNull(p);
        assertNull("Complex property should not be exposed when not enabled in framework properties",
                p.getProperties().get("complexTest:complexItem"));
    }

    private ArrayList<HashMap> createComplexPropertyList(int listSize) {
        ArrayList<HashMap> list = new ArrayList<HashMap>();
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(1234500000000L);
        for (int i = 1; i <= listSize; i++) {
            HashMap<String, Object> map = new HashMap<String, Object>();
            list.add(map);
            map.put("stringProp", "testString" + i);
            map.put("dateProp", cal);
            map.put("enumProp", "ValueA");
            List<String> arrayProp = new ArrayList<String>();
            map.put("arrayProp", arrayProp);
            for (int j = 1; j <= i; j++) {
                arrayProp.add(Integer.toString(j));
            }
            map.put("intProp", 123);
            map.put("boolProp", true);
            map.put("floatProp", 123.45d);

        }
        return list;
    }

    private ArrayList<ObjectNode> createComplexNodeList(int listSize, DateTimeFormat dateTimeFormat) {
        ObjectMapper mapper = new ObjectMapper();
        ArrayList<ObjectNode> jsonObjects = new ArrayList<ObjectNode>();
        for (int i = 1; i <= listSize; i++) {
            ObjectNode jsonObj = mapper.createObjectNode();
            jsonObj.put("stringProp", "testString" + i);
            if (dateTimeFormat.equals(DateTimeFormat.TIME_IN_MILLIS)) {
                jsonObj.put("dateProp", 1234500000000L);
            } else {
                Calendar cal = Calendar.getInstance();
                cal.setTimeInMillis(1234500000000L);
                String dateStr = DateParser.formatW3CDateTime(cal.getTime());
                jsonObj.put("dateProp", dateStr);
            }
            jsonObj.put("enumProp", "ValueA");
            ArrayNode jsonArray = mapper.createArrayNode();
            jsonObj.put("arrayProp", jsonArray);
            for (int j = 1; j <= i; j++) {
                jsonArray.add(Integer.toString(j));
            }
            jsonObj.put("intProp", 123);
            jsonObj.put("boolProp", true);
            jsonObj.put("floatProp", 123.45d);
            jsonObjects.add(jsonObj);
        }
        return jsonObjects;
    }

    private void assertComplexPropertyNodeEquals(Map<String, Object> propMap, JsonNode jsonNode,
            DateTimeFormat dateTimeFormat) throws IOException {
        List<String> nodeKeys = copyIterator(jsonNode.getFieldNames());
        Set<String> propKeys = propMap.keySet();
        assertEquals(nodeKeys.size(), propKeys.size());
        nodeKeys.containsAll(propKeys);
        for (String key : propKeys) {
            Object origVal = propMap.get(key);
            if (origVal instanceof ArrayList || origVal instanceof Object[]) {
                List<Object> origList;
                if (origVal instanceof ArrayList) {
                    origList = (List<Object>) origVal;
                } else {
                    origList = Arrays.asList((Object[]) origVal);
                }
                ArrayNode jsonArray = (ArrayNode) jsonNode.get(key);
                for (int i = 0; i < origList.size(); i++) {
                    assertEquals("Wrong value at key [" + key + "] index [" + i + "]",
                            origList.get(i).toString(), jsonArray.get(i).getValueAsText());
                }
            } else {
                if (origVal instanceof Calendar) {
                    if (DateTimeFormat.TIME_IN_MILLIS.equals(dateTimeFormat)) {
                        origVal = ((Calendar) origVal).getTimeInMillis();
                    } else {
                        origVal = DateParser.formatW3CDateTime(((Calendar) origVal).getTime());
                    }
                }
                assertEquals("Wrong value at key [" + key + "]",
                        origVal.toString(), jsonNode.get(key).getValueAsText());
            }
        }
    }

    private <T> List<T> copyIterator(Iterator<T> iter) {
        List<T> copy = new ArrayList<T>();
        while (iter.hasNext())
            copy.add(iter.next());
        return copy;
    }
}
