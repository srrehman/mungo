package com.mungoae.object;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.ConcurrentModificationException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import com.google.appengine.api.NamespaceManager;
import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.DataTypeUtils;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.EmbeddedEntity;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Text;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.api.datastore.TransactionOptions;
import com.google.appengine.api.datastore.Query.Filter;
import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.appengine.api.datastore.Query.FilterPredicate;
import com.google.appengine.api.datastore.Query.SortDirection;
import com.google.appengine.api.users.User;
import com.google.common.base.Preconditions;
import com.mungoae.DBCollection;
import com.mungoae.DBObject;
import com.mungoae.collection.AbstractDBCollection;
import com.mungoae.common.SerializationException;
import com.mungoae.query.Logical;
import com.mungoae.query.Update.UpdateOperator;
import com.mungoae.serializer.ObjectSerializer;
import com.mungoae.serializer.XStreamSerializer;
import com.mungoae.util.BoundedIterator;
import com.mungoae.util.Tuple;

public abstract class AbstractEntityStore  {
	
	protected static Logger LOG = LogManager.getLogger(AbstractDBCollection.class.getName());	
	protected static DatastoreService _ds;
	protected static TransactionOptions options;
	protected Calendar cal;
	/**
	 * GAE datastore supported types.
	 */
	protected static final Set<Class<?>> GAE_SUPPORTED_TYPES =
	        DataTypeUtils.getSupportedTypes();		
	
	
	protected final String _dbName;
	protected final String _collName;
	protected final ObjectSerializer _serializer;

	public AbstractEntityStore(String namespace, String collection) {
		if (_ds == null) {
			_ds = DatastoreServiceFactory.getDatastoreService();
			options = TransactionOptions.Builder.withXG(true);
			LOG.info("Create a new DatastoreService instance");
		}
		cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));	
		_dbName = namespace;
		_collName = collection;
		_serializer = new XStreamSerializer();
	}
	
	// Helper method
	protected static String buildStringIdFromObject(DBObject obj){
		return createStringIdFromObject(obj.get(DBCollection.MUNGO_DOCUMENT_ID_NAME));
	}
	
	/**
	 * Get <code>String</code> id from a <code>Object</code>
	 * 
	 * @param docId
	 * @return
	 */
	protected static String createStringIdFromObject(Object docId){
		Preconditions.checkNotNull(docId, "Object document id cannot be null");
		if (docId instanceof ObjectId){
			LOG.debug("Create ID String from ObjectID");
			return ((ObjectId) docId).toStringMongod();
		} else if (docId instanceof DBObject) {
			LOG.debug("Get ID from DBObject '_id' property");
			Object id = ((DBObject) docId).get(DBCollection.MUNGO_DOCUMENT_ID_NAME);
			return createStringIdFromObject(id); // TODO - Review this, might cause Stackoverflow error
		} else {
			if (docId instanceof String
					|| docId instanceof Long){
				return String.valueOf(docId);
			} else {
				return docId.toString(); // FIXME not safe
			}
//			try {
//				LOG.debug("Create ID String from arbitrary Object");
//				return serializer.serialize(id);
//			} catch (SerializationException e) {
//				e.printStackTrace();
//			}
		}
	}
	
	/**
	 * Check whether entity with the given properties exist
	 * 
	 * @param props
	 * @return
	 */
	protected boolean containsEntityWithFieldLike(Entity props){
		Preconditions.checkNotNull(props, "Entity cannot be null");
		boolean contains = false;
		Map<String,Object> m = props.getProperties();
		Transaction tx = _ds.beginTransaction();
		try {
			Iterator<String> it = m.keySet().iterator();
			Query q = new Query(_collName);
			while (it.hasNext()){ // build the query
				String propName = it.next();
				Filter filter = new FilterPredicate(propName, 
					FilterOperator.EQUAL, m.get(propName));
				Filter prevFilter = q.getFilter();
				// Note that the Query object is immutable
				q = new Query(_collName).setFilter(prevFilter).setFilter(filter);
				q = q.setKeysOnly();
			}
			PreparedQuery pq = _ds.prepare(q);
			int c = pq.countEntities(FetchOptions.Builder.withDefaults());
			if (c != 0)
				contains = true;
			tx.commit();
		} catch (Exception e) {
			tx.rollback();
		} finally {
		    if (tx.isActive()) {
		        tx.rollback();
		    }
		}		
		return contains;
	}
	
	/**
	 * Validates a given query object
	 * 
	 * @param query
	 */
	protected void validateQuery(DBObject query){
		if (query.keySet().isEmpty()){
			throw new IllegalArgumentException("Empty query");
		}
		for (String field : query.keySet()){
			Object operatorCompareValuePair = query.get(field);
			if (!(operatorCompareValuePair instanceof DBObject)){
				throw new IllegalArgumentException("Invalid query: " 
							+ operatorCompareValuePair);
			}
			LOG.debug("Query reference field=" 
					+ field 
					+ " reference operator/value=" 
					+ operatorCompareValuePair);
		}
	}
	
	/**
	 * Evaluates the operation and return the resulting value
	 * 
	 * @param value
	 * @param updateOp
	 * @return
	 */
	protected Object evalUpdateOperation(Object value, Tuple<UpdateOperator, Object> updateOp){
		UpdateOperator op = updateOp.getFirst();
		Object opValue = updateOp.getSecond();
		if (op == UpdateOperator.INCREMENT){
			if (value instanceof Number && opValue instanceof Number){
				return (Long) value + (Long) opValue;
			} else {
				throw new IllegalArgumentException("Increment operation only allowed for numbers");
			}
		} else if (op == UpdateOperator.DECREMENT){
			if (value instanceof Number && opValue instanceof Number){
				return (Long) value - (Long) opValue;
			} else {
				throw new IllegalArgumentException("Decrement operation only allowed for numbers");
			}	
		} else if (op == UpdateOperator.SET) {
			return updateOp.getSecond();
		} else if (op == UpdateOperator.UNSET) {
			return null;
		} else if (op == UpdateOperator.RENAME) {
			
		} else if (op == UpdateOperator.SET_ON_INSERT) {
			
		}
		return null;
	}
	
	/**
	 * Query for objects matching all the filters
	 * 
	 * @param filters
	 * @return
	 */
	protected Iterator<DBObject> queryObjects(Map<String, Tuple<FilterOperator, Object>> filters){
		Preconditions.checkNotNull(filters, "Null query filter map");
		/**
		 * Map of fields and its matching filter operator and compare value
		 */
		String oldNamespace = NamespaceManager.get();
		NamespaceManager.set(_dbName);
		Iterator<DBObject> it = null;
		try {
			final Iterator<Entity> eit = queryEntitiesLike(filters); 
			it = new Iterator<DBObject>() {
				public void remove() {
					eit.remove();
				}
				public DBObject next() {
					Entity e = eit.next();
					return Mapper.createDBObjectFromEntity(e);
				}
				public boolean hasNext() {
					return eit.hasNext();
				}
			};	
		} catch (Exception e) {
			e.printStackTrace();
			it = null;
		} finally {
			if (oldNamespace != null)
				NamespaceManager.set(oldNamespace);
		}		
		if (it == null){
			LOG.debug("Returning null iterator");
		}
		return it;		
	}

	
	/**
	 * Get a list of entities that matches the properties of a given 
	 * <code>Entity</code>.
	 * This does not include the id.
	 * 
	 * @param entity
	 * @param kind
	 * @return
	 */
	protected Iterator<Entity> queryEntitiesLike(Map<String, 
			Tuple<FilterOperator, Object>> queryParam){
		Preconditions.checkNotNull(queryParam, "Null query object");
		Map<String,Tuple<FilterOperator, Object>> query = validateQuery(queryParam);
		Query q = new Query(_collName);
		for (String propName : query.keySet()){
			Tuple<FilterOperator, Object> filterAndValue = query.get(propName);	
			Filter filter = new FilterPredicate(propName, 
					filterAndValue.getFirst(), filterAndValue.getSecond()); 
			Filter prevFilter = q.getFilter();
			q = new Query(_collName).setFilter(prevFilter).setFilter(filter); 
		}
		PreparedQuery pq = _ds.prepare(q);
		return pq.asIterator();
	}	
	
	/**
	 * @param filters
	 * @param logicalQuery
	 * @param sorts
	 * @param _numToSkip
	 * @param _max
	 * @param fetchSize
	 * @param options
	 * @return
	 */
	protected Iterator<DBObject> __queryObjects(Map<String, Tuple<FilterOperator, Object>> filters,
			List<Tuple<Logical,List<Tuple<FilterOperator,Object>>>> logicalQuery,
			Map<String, Query.SortDirection> sorts, Integer _numToSkip, Integer _max, Integer fetchSize, Integer options){
		if (filters == null){
			filters = new HashMap<String, Tuple<FilterOperator, Object>>();
		}
		/**
		 * Map of fields and its matching filter operator and compare value
		 */
		String oldNamespace = NamespaceManager.get();
		NamespaceManager.set(_dbName);
		Iterator<DBObject> it = null;
		try {
			if (sorts == null){
				sorts = new HashMap<String, SortDirection>();
			}
			final Iterator<Entity> eit = querySortedEntitiesLike(filters, sorts); 
			it = new Iterator<DBObject>() {
				public void remove() {
					eit.remove();
				}
				public DBObject next() {
					Entity e = eit.next();
					return Mapper.createDBObjectFromEntity(e);
				}
				public boolean hasNext() {
					return eit.hasNext();
				}
			};	
		} catch (Exception e) {
			e.printStackTrace();
			it = null;
		} finally {
			if (oldNamespace != null)
				NamespaceManager.set(oldNamespace);
		}		
		if (it == null){
			LOG.debug("Returning null iterator");
		}
		if (_max != null){
			if (_numToSkip != null){
				return new BoundedIterator<DBObject>(_numToSkip, _max, it);
			} else {
				return new BoundedIterator<DBObject>(0, _max, it); 
			}
		} 
		//List asList = Lists.newArrayList(it);
		return it;				
	}	
	
	
	/**
	 * Validates a query before it gets passed into the GAE api.
	 * Replace and/or transform an object into GAE Datastore supported type
	 * 
	 * @param query
	 * @return the validated query object
	 */
	protected Map<String, Tuple<FilterOperator, Object>> validateQuery(
			Map<String, Tuple<FilterOperator, Object>> query) {
		//LOG.debug("Query object type=" + query.getSecond().getClass().getName());	
		Map<String,Object> toReplace = new HashMap<String,Object>();
		Set<Map.Entry<String, Tuple<FilterOperator, Object>>> entrySet = query.entrySet();
		for (Map.Entry<String, Tuple<FilterOperator, Object>> entry : entrySet){
			String field = entry.getKey();
			Tuple<FilterOperator, Object> value = entry.getValue();
			if (value.getSecond() instanceof ObjectId){
				Tuple<FilterOperator, Object> newValue 
					= new Tuple<Query.FilterOperator, Object>(value.getFirst(), 
							((ObjectId)value.getSecond()).toStringMongod());
				toReplace.put(field, newValue);
			} else if (!GAE_SUPPORTED_TYPES.contains(value.getSecond().getClass())) {
				throw new IllegalArgumentException("Unsupported filter compare value: " + value.getSecond().getClass());
			}
		}
		
		Iterator<String> it = toReplace.keySet().iterator();
		while(it.hasNext()){
			String keyToReplace = it.next();
			query.put(keyToReplace, (Tuple<FilterOperator, Object>) toReplace.get(keyToReplace));
		}
		
		return query;
	}

	/**
	 * Builds a query filter from the given <code>Entity</code> property names and values and add sorting from
	 * <code>Map</code> sorts.
	 *  
	 * <br>
	 * <code>
	 *  Query q = new Query(kind).filter(f1).filter(f2).filter(fn).addSort(s).addSort(sn); // and so forth
	 * </code>
	 * 
	 * FIXME - Fix query object filter/sort getting wiped out
	 * 
	 * @param entity
	 * @param sorts
	 * @return
	 */
	protected Iterator<Entity> querySortedEntitiesLike(
			Map<String, Tuple<FilterOperator, Object>> query, Map<String, SortDirection> sorts){
		Preconditions.checkNotNull(query, "Query object can't be null");
		Preconditions.checkNotNull(sorts, "Sort can't be null");
		LOG.debug("Query map="+query.toString());
		
		PreparedQuery pq = null;
		
		// Sort
		Iterator<Map.Entry<String, Query.SortDirection>> sortIterator = sorts.entrySet().iterator(); 
		// Dont execute the while-loop it will clear the iterator
//		while(sortIterator.hasNext()){
//			Map.Entry<String, Query.SortDirection> dir = sortIterator.next();
//			LOG.debug("Sort propName="+dir.getKey() + " direction="+dir.getValue());
//		}
		Query q = new Query(_collName);
		List<Filter> subFilters = new ArrayList<Filter>();
		if (!query.isEmpty()){
			// Apply filters and sorting for fields given in the filter query
			for (String propName : query.keySet()){
				LOG.debug("Filter Property name="+propName);
				Tuple<FilterOperator, Object> filterAndValue = query.get(propName);
				FilterOperator operator = filterAndValue.getFirst();
				Object value = filterAndValue.getSecond();
				Filter filter = null;
				if (propName.equals(DBCollection.MUNGO_DOCUMENT_ID_NAME)){
					filter = new FilterPredicate(Entity.KEY_RESERVED_PROPERTY, operator, 
							KeyStructure.createKey(_collName, String.valueOf(value)));
	 			} else {
	 				filter = new FilterPredicate(propName, operator, value); 
	 			}
				Filter prevFilter = q.getFilter();
				if (sorts.get(propName) != null){
					q = new Query(_collName).setFilter(prevFilter).setFilter(filter)
							.addSort(propName, sorts.get(propName));
					sorts.remove(propName); // remove it
				} else {
					q = new Query(_collName).setFilter(prevFilter).setFilter(filter);
				}
				subFilters.add(filter);			
			}
		} else if (query == null || query.isEmpty()){
			while(sortIterator.hasNext()){
//				Map.Entry<String, SortDirection> sort = sortIterator.next();
//				SortPredicate prevSort = q.getSortPredicates().get(q.getSortPredicates().size()-1); // Unsafe
//				if (prevSort != null){
//					q = new Query(_collName).addSort(prevSort.getPropertyName(), prevSort.getDirection())
//							.addSort(sort.getKey(), sort.getValue());	
//				}
				Map.Entry<String, SortDirection> sort = sortIterator.next();
				q = new Query(_collName).addSort(sort.getKey(), sort.getValue());				
			}
		}		
		pq = _ds.prepare(q);
		Iterator<Entity> res = pq.asIterator(); 
		//List asList = Lists.newArrayList(res);
		return res;
	}	
	
	protected <T> List<T> copyIterator(Iterator<T> it){
		List<T> copy = new ArrayList<T>();
		while (it.hasNext()){
		    copy.add(it.next());
		}
		return copy;
	}
	
	
	/**
	 * Query all entities on a given collection
	 * 
	 * @return
	 */
	protected Iterator<Entity> queryEntities(){
		Query q = new Query(_collName);
		PreparedQuery pq = _ds.prepare(q);
		return pq.asIterator();
	}
	
	protected Iterator<Entity> querySortedEntities(Map<String,Query.SortDirection> sorts){
		PreparedQuery pq = null;
		Iterator<Map.Entry<String, Query.SortDirection>> it = sorts.entrySet().iterator();
		Query q = new Query(_collName);
		while(it.hasNext()){
			Map.Entry<String, Query.SortDirection> entry = it.next();
			q = new Query(_collName)
				.addSort(entry.getKey(), entry.getValue());
	
		}
		pq = _ds.prepare(q);
		return pq.asIterator();		
	}

	/**
	 * Fetch a single entity by <code>Key</code>
	 * 
	 * @param k
	 * @return
	 */
	// FIXME - not returning null for non existing entity
	protected Map<String,Object> queryEntityBy(Key k){
		Preconditions.checkNotNull(k, "Entity key cannot be null");
		
		if (!containsEntityKey(k)){
			return null;
		}
		
		Map<String,Object> doc = null;	
		try {
			Entity e = _ds.get(k);
			doc = new LinkedHashMap<String, Object>();
			Map<String,Object> props = e.getProperties();
			Iterator<Map.Entry<String, Object>> it = props.entrySet().iterator();
			// Preprocess - 
			// Can't putAll directly since List and Map
			while(it.hasNext()){
				Map.Entry<String, Object> entry = it.next();
				String key = entry.getKey();
				Object val = entry.getValue();
				if (val == null){
					doc.put(key, val);
				} else if (val instanceof String
						|| val instanceof Number
						|| val instanceof Boolean
						|| val instanceof Date
						|| val instanceof User) {
					// TODO - Check if String is a serialized object
					if (val instanceof String){
						// Try to deserialize
						if(deserializeString((String)val) != null ){
							val = deserializeString((String)val);
						}
					}
					LOG.debug("Inserting to document key="+key);
					LOG.debug("Inserting to document value type="+val.getClass().getName());
					doc.put(key, val);
				} else if (val instanceof EmbeddedEntity) { // List and Map are stored as EmbeddedEntity internally
					LOG.debug( "Embedded entity found.");
					Object mapOrList = Mapper.getMapOrList((EmbeddedEntity) val);
					if (mapOrList instanceof List){
						LOG.debug( "Embedded List="+mapOrList);
					} else if (mapOrList instanceof Map){
						LOG.debug( "Embedded Map="+mapOrList);
					}
					doc.put(key, mapOrList);	
				} 
			}
			//json.put(ID, e.getKey().getName());
			Object _id = createIdObjectFromString(e.getKey().getName());
			if (_id instanceof ObjectId){
				doc.put(DBCollection.MUNGO_DOCUMENT_ID_NAME, ((ObjectId)_id).toStringMongod()); 
			} else if (_id instanceof Long) {
				doc.put(DBCollection.MUNGO_DOCUMENT_ID_NAME, (Long)_id);
			} else {
				doc.put(DBCollection.MUNGO_DOCUMENT_ID_NAME, _id); 
			}
			
		} catch (EntityNotFoundException e) {
			// Just return null
			doc = null;
		} finally {
			
		}
		LOG.debug("Returning null document");
		return doc;
	}
	
	// Used to try to deserialize a String, if it can't 
	// it will just return null
	protected Object deserializeString(String value){
		try {
			//LOG.debug("Parsing string="+value);
			Object deserialized = _serializer.deserialize(value);
			return deserialized;
		} catch (SerializationException e){
			// do nothing
		} catch (Exception e) {
			// do nothing
		}
		return null;
	}
	
	
	/**
	 * Check if in the current <code>Namespace</code> of the current <code>Kind</code>
	 * contains the entity using a Key.
	 *  
	 * @param key
	 * @return
	 */
	protected boolean containsEntityKey(Key key){
		Preconditions.checkNotNull(key, "Entity key cannot be null");
		Transaction tx = _ds.beginTransaction();
		try {
			Query q 
				= new Query(_collName)
					.setFilter(new FilterPredicate(Entity.KEY_RESERVED_PROPERTY, 
							FilterOperator.EQUAL, key)).setKeysOnly(); 
			PreparedQuery pq = _ds.prepare(q);
			Entity e = pq.asSingleEntity();			
			if (e != null)
				return true;
			tx.commit();
		} catch (Exception e) {
			tx.rollback();
		} finally {
		    if (tx.isActive()) {
		        tx.rollback();
		    }
		}		
		return false;
	}
	
	/**
	 * Check whether entity with the given properties exist having
	 * field value compared with the filter operator
	 * 
	 * 
	 * @param props
	 * @return
	 */
	protected boolean containsEntityLike(Map<String, Tuple<FilterOperator, Object>> query){
		Preconditions.checkNotNull(query, "Query object cannot be null");
		boolean contains = false;
	
		Preconditions.checkNotNull(query, "Null query object");

		Transaction tx = _ds.beginTransaction();
		try {
			Query q = new Query(_collName);
			for (String propName : query.keySet()){
				Tuple<FilterOperator, Object> filterAndValue = query.get(propName);
				Filter filter = new FilterPredicate(propName, 
						filterAndValue.getFirst(), filterAndValue.getSecond()); 
				Filter prevFilter = q.getFilter();
				q = new Query(_collName).setFilter(prevFilter).setFilter(filter); 
				q = q.setKeysOnly();
			}
			PreparedQuery pq = _ds.prepare(q);
			int c = pq.countEntities(FetchOptions.Builder.withDefaults());
			if (c != 0)
				contains = true;
			tx.commit();
		} catch (Exception e) {
			tx.rollback();
		} finally {
		    if (tx.isActive()) {
		        tx.rollback();
		    }
		}		
		return contains;
	}
	
	
	protected void deleteEntity(Key key){
		Preconditions.checkNotNull(key, "Entity key cannot be null");
		Transaction tx = _ds.beginTransaction();
		try {
			_ds.delete(key);
			tx.commit();
		} catch (Exception e) {
			tx.rollback();
		} finally {
		    if (tx.isActive()) {
		        tx.rollback();
		    }
		}
	}	

    protected void setProperty(Entity entity, String key, Object value){
    	Preconditions.checkNotNull(entity, "Entity can't be null");
    	Preconditions.checkNotNull(key, "String key can't be null");
    	Preconditions.checkNotNull(value, "Value can't be null");
	    if (!GAE_SUPPORTED_TYPES.contains(value.getClass())
        && !(value instanceof Blob) && !(value instanceof EmbeddedEntity)) {
        throw new RuntimeException("Unsupported type[class=" + value.
                getClass().getName() + "] in Latke GAE repository");
	    }
	    if (value instanceof String) {
	        final String valueString = (String) value;
	        if (valueString.length()
	            > DataTypeUtils.MAX_STRING_PROPERTY_LENGTH) {
	            final Text text = new Text(valueString);
	
	            entity.setProperty(key, text);
	        } else {
	            entity.setProperty(key, value);
	        }
	    } else if (value instanceof Number
	               || value instanceof Date
	               || value instanceof Boolean
	               || GAE_SUPPORTED_TYPES.contains(value.getClass())) {
	        entity.setProperty(key, value);
	    } else if (value instanceof EmbeddedEntity) {
	    	entity.setProperty(key, value);
	    } else if (value instanceof Blob) {
	        final Blob blob = (Blob) value;
	        entity.setProperty(key,
	                           new com.google.appengine.api.datastore.Blob(
	                blob.getBytes()));
	    }  
    }

	/**
	 * Create a <clas>Object</code> id from a given string. 
	 * It first try to deserialize the String with XStream if it
	 * fails it creates a new ObjectId with the given String. Since
	 * there is only two type of String id that is stored using Mungo
	 * which is a ObjectId string and a XStream serialized Object.
	 * 
	 * FIXME - Bug, when a String docId is a number string
	 * it is interpreted as Long
	 * 
	 * @param docId
	 * @return
	 */
	protected Object createIdObjectFromString(String docId){
		Object id = docId;
		try {
			//id = serializer.deserialize(uniqueId);
			id = Long.valueOf(docId);
		} catch (NumberFormatException e) {
			try {
				id = new ObjectId(docId);
			} catch (IllegalArgumentException e2) {
				id = String.valueOf(docId);
			}
		} 
		return id;
	}

	
	/**
	 * <p>
	 * Create an entity for a a <code>Map</code>
	 * <br>
	 * <br>
	 * Note that this method does not enforce any rule in persisting an entity. 
	 * It however may call subsequent datastore <code>put</code>s as necessary when
	 * dealing with "reference" documents. 
	 * <br>
	 * <br>
	 * It also does not restrict the operation on a specific namespace, 
	 * it is however should be managed by that method that calls this method.
	 * <br>
	 * <br>
	 * This method expects the id of the Entity to be in the Map with key "_id"
	 * <br>
	 * <br>
	 * This method also expects that the <code>Map</code> entries are already pre-processed to 
	 * GAE types, otherwise those properties will get thrown out. 
	 * </p>
	 * 
	 * @param parent when provided becomes the parent key of the created Entity, can be set to null
	 * @param obj
	 */
	protected Key persistEntity(Key parent, Map<String,Object> obj) {	 
		Key entityKey = null;
		try {
			Object id = obj.get(DBCollection.MUNGO_DOCUMENT_ID_NAME);
			Entity e = new Entity(
					parent == null ? KeyStructure.createKey(_collName, createStringIdFromObject(id)) : parent);  
			// Clean up the objectId (since the DS have ID field)
			// and since it is already 'copied' into the Entity
			obj.remove(DBCollection.MUNGO_DOCUMENT_ID_NAME);
			Iterator<String> it = obj.keySet().iterator();
			while (it.hasNext()){
				String key = it.next();
				Object value = obj.get(key);
				if (value == null){
					e.setProperty(key, null);
				} else if (value instanceof ObjectId) {
					// FIXME - Check if this is correct
					setProperty(e, key, ((ObjectId) value).toStringMongod());
				} else if (value instanceof String) {
					setProperty(e, key, value);
				} else if(value instanceof Number) {
					setProperty(e, key, value);
				} else if(value instanceof Boolean) {
					setProperty(e, key, value);
				} else if(value instanceof Date) {
					setProperty(e, key, value);
				} else if(value instanceof User) { // GAE support this type
					setProperty(e, key, value);
				} else if(value instanceof List) {
					LOG.debug( "Processing List value");
					setProperty(e, key, Mapper.createEmbeddedEntityFromList(parent, (List) value));
				} else if(value instanceof Map){
					// TODO: Need to deal with sub-documents Object id
					LOG.debug( "Processing Map value");
					setProperty(e, key, Mapper.createEmbeddedEntityFromMap(parent, (Map) value));
				} else {
					//throw new RuntimeErrorException(null, "Unsupported value type: " + value.getClass().getName());
					String serialized = _serializer.serialize(value);
					setProperty(e, key, serialized);
				}
			}	
			LOG.debug( "Persisting entity [" 
					+ e.getKey().getName() + "] in [" + e.getNamespace() + "][" + e.getKind() + "]");
			entityKey = _ds.put(e);
			LOG.debug( "Persisted entity [" + e + "] with key=" + entityKey);
			LOG.debug( "Raw id was="+id);
		} catch (ConcurrentModificationException e){
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return entityKey;
	}		
	
	
}
