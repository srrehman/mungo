package com.pagecrumb.joongo.collection;

import java.io.Serializable;

/**
 * <p>WriteConcern control the acknowledgment of write operations with various options.
 * <p>
 * <b>w</b>
 * <ul>
 *  <li>-1 = Don't even report write errors </li>
 *  <li> 0 = Don't wait for the datastore to be consistent</li>
 *  <li> 1 = Wait for datastore to be consistent, until timeout</li>
 *  <li> 2+= Wait for datastore to be consistent, indefinitely </li>
 * </ul>
 * <b>wtimeout</b> how long to wait for slaves before failing
 * <ul>
 *   <li>0: indefinite </li>
 *   <li>greater than 0: ms to wait </li>
 * </ul>
 * </p>
 * <p>
 * 
 * @author Kerby Martino<kerbymart@gmail.com>
 *
 */
public class WriteConcern implements Serializable {
	private static final long serialVersionUID = 1L;
	Object _w = 0;
	int _wtimeout = 0;
	
	public final static WriteConcern ERRORS_IGNORED = new WriteConcern(-1);

	public final static WriteConcern NONE = new WriteConcern(0);
	
	public final static WriteConcern WAIT_UNTIL_TIMETOUT = new WriteConcern(1);
	
	public final static WriteConcern WAIT_UNTIL_PERSISTED = new WriteConcern(2);

    public WriteConcern(){
        this(0, 0, false, false);
    }

    public WriteConcern(int w){
        this(w, 0, false, false);
    }
        
    /**
     * Creates a WriteConcern object.
     * 
     * @param w number of writes
     * @param wtimeout timeout for write operation
     * @param c whether writes should wait for a the the Datastore to become consistent
     * @param continueOnInsertError if batch inserts should continue after the first error
     */    
    public WriteConcern( int w, int wtimeout, boolean c, boolean continueOnInsertError){
       _w = w;
       _wtimeout = wtimeout;
    }
    
	public String getError() {
		return "";
	}
	public Object getField(String name){
		return null;
	}
	public WriteConcern getLastConcern(){
		return null;
	}
	public int getN(){
		return 0;
	}
	public boolean isLazy(){
		return false;
	}
	
	@Override
	public String toString() {
		return "";
	}
	
	public int getW() {
		return (Integer) _w;
	}
}
