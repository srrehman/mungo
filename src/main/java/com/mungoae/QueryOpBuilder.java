package com.mungoae;

class QueryOpBuilder {

    private DBObject query;
	private DBObject orderBy;
	private DBObject specialFields;

	public QueryOpBuilder(){
	}

	/**
	 * Adds the query clause to the operation
	 * @param query
	 * @return
	 */
	public QueryOpBuilder addQuery(DBObject query){
		this.query = query;
		return this;
	}

	/**
	 * Adds the orderby clause to the operation
	 * @param orderBy
	 * @return
	 */
	public QueryOpBuilder addOrderBy(DBObject orderBy){
		this.orderBy = orderBy;
		return this;	
	}

	/**
	 * Adds special fields to the operation
	 * @param specialFields
	 * @return
	 */
	public QueryOpBuilder addSpecialFields(DBObject specialFields){
		this.specialFields = specialFields;
		return this;
	}

	/**
	 * Constructs the query operation DBObject
	 * @return DBObject representing the query command to be sent to server
	 */
	public DBObject get() {
            DBObject lclQuery = query;

            //must always have a query
            if (lclQuery == null) {
                lclQuery = new BasicDBObject();
            }

            if (hasSpecialQueryFields()) {
                DBObject queryop = (specialFields == null ? new BasicDBObject() : specialFields);
                addToQueryObject(queryop, "$query", lclQuery, true);
                addToQueryObject(queryop, "$orderby", orderBy, false);
                return queryop;
            }

            return lclQuery;
        }

    private boolean hasSpecialQueryFields(){
        if ( specialFields != null )
            return true;
        if ( orderBy != null && orderBy.keySet().size() > 0 )
            return true;
        return false;
    }

	/**
	 * Adds DBObject to the operation
	 * @param dbobj DBObject to add field to
	 * @param field name of the field
	 * @param obj object to add to the operation.  Ignore if <code>null</code>.
	 * @param sendEmpty if <code>true</code> adds obj even if it's empty.  Ignore if <code>false</code> and obj is empty.
	 * @return
	 */
	private void addToQueryObject(DBObject dbobj, String field, DBObject obj, boolean sendEmpty) {
		if (obj == null)
			return;

		if (!sendEmpty && obj.keySet().size() == 0)
			return;

		addToQueryObject(dbobj, field, obj);
	}

	/**
	 * Adds an Object to the operation
	 * @param dbobj DBObject to add field to
	 * @param field name of the field
	 * @param obj Object to be added.  Ignore if <code>null</code>
	 * @return
	 */
	private void addToQueryObject(DBObject dbobj, String field, Object obj) {
		if (obj == null)
			return;
		dbobj.put(field, obj);
	}

}