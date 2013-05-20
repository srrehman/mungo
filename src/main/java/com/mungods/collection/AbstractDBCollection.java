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
package com.mungods.collection;

import java.util.Calendar;
import java.util.Set;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.appengine.api.datastore.DataTypeUtils;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.TransactionOptions;
/**
 * 
 * @author Kerby Martino<kerbymart@gmail.com>
 *
 */
public abstract class AbstractDBCollection {
	private static final Logger LOG 
		= Logger.getLogger(AbstractDBCollection.class.getName());	
	protected static DatastoreService _ds;
	protected static TransactionOptions options;
	protected Calendar cal;
	/**
	 * GAE datastore supported types.
	 */
	protected static final Set<Class<?>> GAE_SUPPORTED_TYPES =
	        DataTypeUtils.getSupportedTypes();		
	
	public AbstractDBCollection(String namespace) {
		if (_ds == null) {
			_ds = DatastoreServiceFactory.getDatastoreService();
			options = TransactionOptions.Builder.withXG(true);
			LOG.log(Level.INFO, "Create a new DatastoreService instance");
		}
		cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));			
	}
	

}
