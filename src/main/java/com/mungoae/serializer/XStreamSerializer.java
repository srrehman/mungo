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
package com.mungoae.serializer;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.mungoae.common.SerializationException;
import com.mungoae.object.ObjectStore;
import com.thoughtworks.xstream.XStream;
/**
 * Object serializer that user XStreamGae, a port of XStream to GAE
 * 
 * @author Kerby Martino<kerbymart@gmail.com>
 *
 */
public class XStreamSerializer implements ObjectSerializer {
	
	private static Logger LOG = LogManager.getLogger(XStreamSerializer.class.getName());

	XStream xstream;
	
	public XStreamSerializer() {
		xstream = new XStreamGae();
	}

	public String serialize(Object obj) throws SerializationException {
		try {
			String serialized = xstream.toXML(obj); 
			LOG.info("Serialized object="+serialized);
			return serialized;
		} catch (Exception e) {
			throw new SerializationException();
		}
	}

	public Object deserialize(String obj) throws SerializationException {
		try {
			return xstream.fromXML(obj); 
		} catch (Exception e) {
			throw new SerializationException();
		}
	}

}
