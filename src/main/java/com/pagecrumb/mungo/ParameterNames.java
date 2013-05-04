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
package com.pagecrumb.mungo;
/**
 * Common keys
 * 
 * @author Kerby Martino<kerbymart@gmail.com>
 * @since 0.0.1
 * @version 0.0.1
 */
public interface ParameterNames {
	/**
	 * The admin namespace contains all the Database entities
	 * and Collection entities
	 */
	public static final String ADMIN_NAMESPACE = "Admin";
	public static final String DATABASE_KIND = "Database";
	public static final String COLLECTION_KIND = "Collection";
	
	public static final String DATABASE_NAME = "_dbName";	
	public static final String COLLECTION_NAME = "_colName";
	
	// internal attributes & keywords for collecction
	public static final String ID = "_id";
	public static final String DOC_COUNT = "_count";
	public static final String UPDATES = "_updates";
	public static final String REVISION = "_rev";
	public static final String CREATED = "_created";
	public static final String UPDATED = "_updated";
	
	public static final String OFFSET = "offset";
	public static final String LIMIT = "limit";
	public static final String TOTAL_ROWS = "total_rows"; 
	public static final String ROWS = "rows";
	
	
	//public static final String OBJECT_ID = "_id";
}
