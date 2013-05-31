package com.mungods.object;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.Entity;
import com.google.common.base.Preconditions;
import com.mungods.BasicDBObject;
import com.mungods.DBCollection;
import com.mungods.DBCursor;
import com.mungods.DBObject;
/**
 * Executable object. 
 * Get form the <code>XObjectFactory</code>.
 * <br>
 * <br>
 * It executes a command directly applying to the GAE datastore.
 * 
 * @author Kerby Martino <kerbymart@gmail.com>
 *
 */
public class GAEObject {
	
	private static final Logger LOG 
		= Logger.getLogger(GAEObject.class.getName());
	
	public static final String INSERT = "insert";
	public static final String SAVE = "save";
	public static final String UPDATE = "update";
	public static final String FIND = "find";
	//private static final String FIND_ONE = "$findOne";
	public static final String REMOVE = "remove";
	
	private final Set<String> commands = new HashSet<String>();
	
	private final String _ns;
	private final String _kind;
	
	private List<DBObject> _docs;
	private DBObject _query = null;
	private DBObject _fields = null;
	private DBObject _lastError = null;
	
	private boolean _upsert = false;
	private boolean _justOne = true; 
	private boolean _multi = false;
	
	private DBCursor _curr = null; 
	private DBObject _singleResult = null;
	
	private String _cmd; 
	
	public GAEObject(final String namespace, final String kind){
		_ns = namespace;
		_kind = kind;
		commands.add(INSERT);
		commands.add(SAVE);
		commands.add(FIND);
		commands.add(REMOVE);
		_lastError = new BasicDBObject("ok", true);
	}
	
	public GAEObject justOne(boolean justOne){
		_justOne = justOne;
		return this;
	}

	
	public GAEObject setDoc(DBObject doc){
		_docs = new ArrayList<DBObject>();
		_docs.add(doc);
		return this;
	}
	
	public GAEObject setDoc(List<DBObject> docs){
		_docs = docs;
		return this;
	}
	
	public GAEObject setQuery(DBObject query){
		_query = query;
		return this;
	}
	
	public GAEObject setFields(DBObject fields){
		_fields = fields;
		return this;
	}
	
	public GAEObject setCommand(String cmd){
		_cmd = cmd;
		return this;
	}
	
	public GAEObject execute() {
		_execute();
		return this;
	}

	private void _execute() {

		if (_cmd == null){
			throw new IllegalArgumentException("Command is null");
		} else if (!commands.contains(_cmd)){
			_lastError = new BasicDBObject("$err", "Unknown command");
			return;
		}
		
		// Remove since for insert _docs is null
		//Preconditions.checkNotNull(_docs, "Document(s) can't be null");
		
		if (_cmd.equalsIgnoreCase(INSERT)){
			if (_justOne){
				DBObject doc = _docs.get(0); 
				Object id = store().persistObject(doc);
			} else {
				// execute in transaction
				boolean success = false;
				int count = 0;
				try { 
					for(DBObject doc : _docs){
						store().persistObject(doc);
					}
				} catch (Exception e) {
					// TODO: handle exception
				}
			}
		} else if (_cmd.equalsIgnoreCase(SAVE)) {
			
		} else if (_cmd.equalsIgnoreCase(UPDATE)) {
			
		} else if (_cmd.equalsIgnoreCase(FIND)){
			if (_justOne){
				if (store().containsKey(_query.get("_id"))){ 
					_singleResult = store().getObject(_query);
				} else {
					_singleResult = null;
				}
			} else {
				Preconditions.checkNotNull(_query, "Query cannot be null");
				Iterator<DBObject> it = store().getObjectsLike(_query);
				if (it.hasNext()){
					//_curr = new DBCursor(it);
				}
			}
		} else if (_cmd.equalsIgnoreCase(REMOVE)){
			if (_justOne){
				DBObject forDelete = _docs.get(0); 
				store().deleteObject(forDelete);
			}
		}
	}
	
	public DBCursor getResult(){
		if (_curr == null)
			LOG.warning("Returning null cursor");
		return _curr;
	}
	
	public DBObject getSingleResult() {
		return _singleResult;
	}
	
	public DBObject getLastError(){
		return _lastError;
	}
	
	// TODO - Check if obj has "_id" as it is required
	private DBObject copyFieldsOnly(DBObject obj, DBObject fields){
		DBObject copy = null;
		Iterator<String> it = fields.keySet().iterator();
		while (it.hasNext()){
			if (copy == null){
				copy = new BasicDBObject();
			}
			String key = it.next();
			copy.put(key, obj.get(key));
		}
		return copy;
	}
	
	private ObjectStore store(){
		return ObjectStore.get(_ns, _kind);
	}
	
}
