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
package com.pagecrumb.joongo.common;

@SuppressWarnings("serial")
public class JoongoException extends Exception {
	public JoongoException() {
		super();
	}
	public JoongoException(String message){
		super(message);
	}
	
	public JoongoException(String message, Throwable cause){
		super(message, cause);
	}	

	public JoongoException(int code, String message){
		super("Exception " + code + " " + message);
	}	
}
