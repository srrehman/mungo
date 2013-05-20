package com.mungods.shell;

import java.util.Calendar;
import java.util.Set;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.appengine.api.appidentity.AppIdentityService;
import com.google.appengine.api.appidentity.AppIdentityServiceFactory;
import com.google.appengine.api.datastore.DataTypeUtils;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.TransactionOptions;
import com.google.inject.Singleton;
import com.mungods.Mungo;

/**
 * Connector encapsulates database operations to the datastore. 
 * It can connect to the local Datastore in which this <code>Mungo</code> instance is running.
 * <br>
 * <br>
 * Future version is intended to be able to connect to a remote Mungo datastore 
 * through the standard Rest API.
 * 
 * @author Kerby Martino <kerbymart@gmail.com>
 *
 */
@Singleton
public class Connector {
	
	private static final Logger logger 
		= Logger.getLogger(Connector.class.getName());
	
	protected AppIdentityService _appIdentity; 
    protected static DatastoreService _ds;
	protected static TransactionOptions options;
	protected Calendar cal;	
	/**
	 * GAE datastore supported types.
	 */
	private static final Set<Class<?>> GAE_SUPPORTED_TYPES =
	        DataTypeUtils.getSupportedTypes();	
	
	public Connector(){
		if (_appIdentity == null){
			_appIdentity = AppIdentityServiceFactory.getAppIdentityService();
		}
		if (_ds == null) {
			_ds = DatastoreServiceFactory.getDatastoreService();
			options = TransactionOptions.Builder.withXG(true);
			logger.log(Level.INFO, "Create a new DatastoreService instance");
		}
		cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));				
	}
	
	public GAEObjectFactory getFactory(){
		return new GAEObjectFactory();
	}
	
	public Shell getLocal(){
		throw new UnsupportedOperationException();
	}
	public Shell getRemote(String url){
		throw new UnsupportedOperationException();
	}
	
}
