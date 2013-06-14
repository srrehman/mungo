package com.mungoae.gridfs;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.bson.types.ObjectId;

import com.google.appengine.api.files.FileService;
import com.google.appengine.api.files.FileServiceFactory;
import com.mungoae.BasicDBObject;
import com.mungoae.DB;
import com.mungoae.DBCollection;
import com.mungoae.DBCursor;
import com.mungoae.DBObject;

public class GridFS {
	
	protected String _bucketName;
	protected DB _db;
	public DBCollection _chunkCollection;
	public DBCollection _filesCollection;
	public static String DEFAULT_BUCKET = "fs";
	public static int DEFAULT_CHUNKSIZE = 50 * 1024; 
	public static long MAX_CHUNKSIZE = 50 * 1024; // 50MB Blobstore limit
	
	private FileService fileService = FileServiceFactory.getFileService();
	private static final String DEFAULT_FILE_MIME_TYPE = "binary/octet-stream";
	
	public GridFS(DB db){
		this(db, DEFAULT_BUCKET);
	}
	
	public GridFS(DB db, String bucket){
		_db = db;
		_bucketName = bucket;
		_filesCollection = _db.getCollection(_bucketName + ".files"); 
		_chunkCollection = _db.getCollection(_bucketName + ".chunks"); 
		//_chunkCollection.ensureIndex( BasicDBObjectBuilder.start().add( "files_id" , 1 ).add( "n" , 1 ).get() );
		_filesCollection.setObjectClass(GridFSDBFile.class);
	}

    /**
     *   Returns a cursor for this filestore
     *
     * @return cursor of file objects
     */
    public DBCursor getFileList(){
        return _filesCollection.find().sort(new BasicDBObject("filename",1));
    }

    /**
     *   Returns a cursor for this filestore
     *
     * @param query filter to apply
     * @return cursor of file objects
     */
    public DBCursor getFileList( DBObject query ){
        return _filesCollection.find(query).sort(new BasicDBObject("filename",1));
    }


    // --------------------------
    // ------ reading     -------
    // --------------------------

    public GridFSDBFile find( ObjectId id ){
        return findOne( id );
    }
    public GridFSDBFile findOne( ObjectId id ){
        return findOne( new BasicDBObject( "_id" , id ) );
    }
    public GridFSDBFile findOne( String filename ){
        return findOne( new BasicDBObject( "filename" , filename ) );
    }
    public GridFSDBFile findOne( DBObject query ){
        return _fix( _filesCollection.findOne( query ) );
    }

    public List<GridFSDBFile> find( String filename ){
        return find( new BasicDBObject( "filename" , filename ) );
    }
    public List<GridFSDBFile> find( DBObject query ){
        List<GridFSDBFile> files = new ArrayList<GridFSDBFile>();

        DBCursor c = _filesCollection.find( query );
        while ( c.hasNext() ){
            files.add( _fix( c.next() ) );
        }
        return files;
    }

    private GridFSDBFile _fix( Object o ){
        if ( o == null )
            return null;

        if ( ! ( o instanceof GridFSDBFile ) )
            throw new RuntimeException( "somehow didn't get a GridFSDBFile" );

        GridFSDBFile f = (GridFSDBFile)o;
        f._fs = this;
        return f;
    }


    // --------------------------
    // ------ remove      -------
    // --------------------------

    public void remove( ObjectId id ){
        _filesCollection.remove( new BasicDBObject( "_id" , id ) );
        _chunkCollection.remove( new BasicDBObject( "files_id" , id ) );
    }
    
    public void remove( String filename ){
        remove( new BasicDBObject( "filename" , filename ) );
    }

    public void remove( DBObject query ){
        for ( GridFSDBFile f : find( query ) ){
            f.remove();
        }
    }
    

    // --------------------------
    // ------ writing     -------
    // --------------------------

    /**
     * after calling this method, you have to call save() on the GridFSInputFile file
     */
    public GridFSInputFile createFile( byte[] data ){
        return createFile( new ByteArrayInputStream( data ) );
    }


    /**
     * after calling this method, you have to call save() on the GridFSInputFile file
     */
    public GridFSInputFile createFile( File f )
        throws IOException {
        return createFile( new FileInputStream( f ) , f.getName() );
    }

    /**
     * after calling this method, you have to call save() on the GridFSInputFile file
     */
    public GridFSInputFile createFile( InputStream in ){
        return createFile( in , null );
    }

    /**
     * after calling this method, you have to call save() on the GridFSInputFile file
     * on that, you can call setFilename, setContentType and control meta data by modifying the 
     *   result of getMetaData
     */
    public GridFSInputFile createFile( InputStream in , String filename ){
        return new GridFSInputFile( this , in , filename );
    }
}
