/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.mahout.cf.taste.impl.model.accumulo;

import com.google.common.base.Preconditions;
import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.FloatSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.service.OperationType;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ConsistencyLevelPolicy;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.CountQuery;
import me.prettyprint.hector.api.query.SliceQuery;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.mahout.cf.taste.common.NoSuchItemException;
import org.apache.mahout.cf.taste.common.NoSuchUserException;
import org.apache.mahout.cf.taste.common.Refreshable;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.Cache;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.common.Retriever;
import org.apache.mahout.cf.taste.impl.model.GenericItemPreferenceArray;
import org.apache.mahout.cf.taste.impl.model.GenericUserPreferenceArray;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.PreferenceArray;

import java.io.Closeable;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.RuntimeErrorException;

/**
 * <p>A {@link DataModel} based on a Cassandra keyspace. By default it uses keyspace "recommender" but this
 * can be configured. Create the keyspace before using this class; this can be done on the Cassandra command
 * line with a command linke {@code create keyspace recommender;}.</p>
 *
 * <p>Within the keyspace, this model uses four column families:</p>
 *
 * <p>First, it uses a column family called "users". This is keyed by the user ID as an 8-byte long.
 * It contains a column for every preference the user expresses. The column name is item ID, again as
 * an 8-byte long, and value is a floating point value represnted as an IEEE 32-bit floating poitn value.</p>
 *
 * <p>It uses an analogous column family called "items" for the same data, but keyed by item ID rather
 * than user ID. In this column family, column names are user IDs instead.</p>
 *
 * <p>It uses a column family called "userIDs" as well, with an identical schema. It has one row under key
 * 0. IT contains a column for every user ID in th emodel. It has no values.</p>
 *
 * <p>Finally it also uses an analogous column family "itemIDs" containing item IDs.</p>
 *
 * <p>Each of these four column families needs to be created ahead of time. Again the
 * Cassandra CLI can be used to do so, with commands like {@code create column family users;}.</p>
 *
 * <p>Note that this thread uses a long-lived Cassandra client which will run until terminated. You
 * must {@link #close()} this implementation when done or the JVM will not terminate.</p>
 *
 * <p>This implementation still relies heavily on reading data into memory and caching,
 * as it remains too data-intensive to be effective even against Cassandra. It will take some time to
 * "warm up" as the first few requests will block loading user and item data into caches. This is still going
 * to send a great deal of query traffic to Cassandra. It would be advisable to employ caching wrapper
 * classes in your implementation, like {@link org.apache.mahout.cf.taste.impl.recommender.CachingRecommender}
 * or {@link org.apache.mahout.cf.taste.impl.similarity.CachingItemSimilarity}.</p>
 */
public final class AccumuloDataModel implements DataModel, Closeable {

  static final String USERS_CF = "users";
  static final String ITEMS_CF = "items";
  static final String USER_IDS_CF = "userIDs";
  static final String ITEM_IDS_CF = "itemIDs";

  private static final long ID_ROW_KEY = 0L;
  private static final byte[] EMPTY = new byte[0];

  private final Cache<Long,PreferenceArray> userCache;
  private final Cache<Long,PreferenceArray> itemCache;
  private final Cache<Long,FastIDSet> itemIDsFromUserCache;
  private final Cache<Long,FastIDSet> userIDsFromItemCache;
  private final AtomicReference<Integer> userCountCache;
  private final AtomicReference<Integer> itemCountCache;

  Authorizations auths;
  String table;
  Connector conn;
  BatchWriter writer;
  
  /**
   * @param host Cassandra server host name
   * @param port Cassandra server port
   * @param keyspaceName name of Cassandra keyspace to use
 * @throws AccumuloSecurityException 
 * @throws AccumuloException 
 * @throws TableExistsException 
   */
  public AccumuloDataModel(String instance, String zookeepers, String user, String pass, String table, Authorizations auths) throws AccumuloException, AccumuloSecurityException, TableExistsException {
    
	  Instance inst = new ZooKeeperInstance(instance, zookeepers);
	  this.conn = inst.getConnector(user, pass);
	  this.table = table;
	  this.auths = auths;
	  
	  if(!conn.tableOperations().exists(table))
	  {
		  conn.tableOperations().create(table);
		  // TODO: setup Locality groups...
	  }
	  
		try {
			writer = conn.createBatchWriter(table, 50000000, 10000, 10);
		} catch (TableNotFoundException e) {
			throw new RuntimeException("failed creating BatchWrier");
		}

    userCache = new Cache<Long,PreferenceArray>(new UserPrefArrayRetriever(), 1 << 20);
    itemCache = new Cache<Long,PreferenceArray>(new ItemPrefArrayRetriever(), 1 << 20);
    itemIDsFromUserCache = new Cache<Long,FastIDSet>(new ItemIDsFromUserRetriever(), 1 << 20);
    userIDsFromItemCache = new Cache<Long,FastIDSet>(new UserIDsFromItemRetriever(), 1 << 20);
    userCountCache = new AtomicReference<Integer>(null);
    itemCountCache = new AtomicReference<Integer>(null);
  }

  @Override
  public LongPrimitiveIterator getUserIDs() {
   
	  return getIds(USER_IDS_CF, USER_IDS_CF).iterator();
  }
  
  private FastIDSet getIds(String row, String cf)
  {
	  Scanner scan;
		try {
			scan = conn.createScanner(table, auths);
		} catch (TableNotFoundException e1) {
			throw new RuntimeException("Failed to create scanner");
		}
		  scan.fetchColumnFamily(new Text(cf));
		  scan.setRange(new Range(row));
		  
		  FastIDSet userIDs = new FastIDSet();
		  for(Entry<Key, Value> e : scan)
		  {
			  userIDs.add(Long.parseLong(e.getKey().getColumnQualifier().toString()));
		  }
	    return userIDs;
  }
  
  @Override
  public PreferenceArray getPreferencesFromUser(long userID) throws TasteException {
    return userCache.get(userID);
  }

  @Override
  public FastIDSet getItemIDsFromUser(long userID) throws TasteException {
    return itemIDsFromUserCache.get(userID);
  }

  @Override
  public LongPrimitiveIterator getItemIDs() {
	  return getIds(ITEM_IDS_CF, ITEM_IDS_CF).iterator();
  }

  @Override
  public PreferenceArray getPreferencesForItem(long itemID) throws TasteException {
    return itemCache.get(itemID);
  }

  @Override
  public Float getPreferenceValue(long userID, long itemID) {
	  
	  Scanner scan;
		try {
			scan = conn.createScanner(table, auths);
		} catch (TableNotFoundException e1) {
			throw new RuntimeException("Failed to create scanner");
		}
		  scan.fetchColumnFamily(new Text(USERS_CF));
		  scan.setRange(new Range(
				  new Key(Long.toString(userID), USERS_CF, Long.toString(itemID)), 
				  new Key(Long.toString(userID), USERS_CF, Long.toString(itemID)+"\0")));
		  
		  for(Entry<Key, Value> e : scan)
		  {
			  return Float.parseFloat(e.getValue().toString());
		  }
		  return null;
  }

  @Override
  public Long getPreferenceTime(long userID, long itemID) {
	  Scanner scan;
		try {
			scan = conn.createScanner(table, auths);
		} catch (TableNotFoundException e1) {
			throw new RuntimeException("Failed to create scanner");
		}
		  scan.fetchColumnFamily(new Text(USERS_CF));
		  scan.setRange(new Range(
				  new Key(Long.toString(userID), USERS_CF, Long.toString(itemID)), 
				  new Key(Long.toString(userID), USERS_CF, Long.toString(itemID)+"\0")));
		  
		  for(Entry<Key, Value> e : scan)
		  {
			  return e.getKey().getTimestamp();
		  }
		  return null;
  }

  @Override
  public int getNumItems() {
    Integer itemCount = itemCountCache.get();
    if (itemCount == null) {
    	
    	// TODO: use a Combiner to count the item IDs
    	itemCount = getIds(ITEM_IDS_CF, ITEM_IDS_CF).size();      
      itemCountCache.set(itemCount);
    }
    return itemCount;
  }

  @Override
  public int getNumUsers() {
    Integer userCount = userCountCache.get();
    if (userCount == null) {
      
    	// TODO: use a Combiner to count the item IDs
      userCount = getIds(USER_IDS_CF, USER_IDS_CF).size();
      userCountCache.set(userCount);
    }
    return userCount;
  }

  @Override
  public int getNumUsersWithPreferenceFor(long itemID) throws TasteException {
    /*
    CountQuery<Long,Long> query = HFactory.createCountQuery(keyspace, LongSerializer.get(), LongSerializer.get());
    query.setColumnFamily(ITEMS_CF);
    query.setKey(itemID);
    query.setRange(null, null, Integer.MAX_VALUE);
    return query.execute().get();
     */
    return userIDsFromItemCache.get(itemID).size();
  }

  @Override
  public int getNumUsersWithPreferenceFor(long itemID1, long itemID2) throws TasteException {
    FastIDSet userIDs1 = userIDsFromItemCache.get(itemID1);
    FastIDSet userIDs2 = userIDsFromItemCache.get(itemID2);
    return userIDs1.size() < userIDs2.size() ?
        userIDs2.intersectionSize(userIDs1) :
        userIDs1.intersectionSize(userIDs2);
  }

  @Override
  public void setPreference(long userID, long itemID, float value) {

    if (Float.isNaN(value)) {
      value = 1.0f;
    }
    
    long now = System.currentTimeMillis();

    String uID = Long.toString(userID);
    String iID = Long.toString(itemID);
    String pref = Float.toString(value);
    
    try {
		Mutation mut = new Mutation(USER_IDS_CF);
		mut.put(USER_IDS_CF, uID, now, "1");
		writer.addMutation(mut);
		
		mut = new Mutation(ITEM_IDS_CF);
		mut.put(ITEM_IDS_CF, iID, now, "1");
		writer.addMutation(mut);
		
		mut = new Mutation(uID);
		mut.put(USERS_CF, iID, now, pref);
		writer.addMutation(mut);
		
		mut = new Mutation(iID);
		mut.put(ITEMS_CF, uID, now, pref);
		
		writer.addMutation(mut);
		
		
		writer.flush();
	} catch (MutationsRejectedException e) {
		throw new RuntimeException("failed adding Mutations");
	}
  }

  @Override
  public void removePreference(long userID, long itemID) {

	    String uID = Long.toString(userID);
	    String iID = Long.toString(itemID);
	    
	    try {
			Mutation mut = new Mutation(USER_IDS_CF);
			mut.putDelete(USER_IDS_CF, uID);
			writer.addMutation(mut);
			
			mut = new Mutation(ITEM_IDS_CF);
			mut.putDelete(ITEM_IDS_CF, iID);
			writer.addMutation(mut);
			
			mut = new Mutation(uID);
			mut.putDelete(USERS_CF, iID);
			writer.addMutation(mut);
			
			mut = new Mutation(iID);
			mut.putDelete(ITEMS_CF, uID);			
			writer.addMutation(mut);
			
			writer.flush();
		} catch (MutationsRejectedException e) {
			throw new RuntimeException("failed adding Mutations");
		}
	  
    // Not deleting from userIDs, itemIDs though
  }

  /**
   * @return true
   */
  @Override
  public boolean hasPreferenceValues() {
    return true;
  }

  /**
   * @return Float#NaN
   */
  @Override
  public float getMaxPreference() {
    return Float.NaN;
  }

  /**
   * @return Float#NaN
   */
  @Override
  public float getMinPreference() {
    return Float.NaN;
  }

  @Override
  public void refresh(Collection<Refreshable> alreadyRefreshed) {
    userCache.clear();
    itemCache.clear();
    userIDsFromItemCache.clear();
    itemIDsFromUserCache.clear();
    userCountCache.set(null);
    itemCountCache.set(null);
  }

  @Override
  public String toString() {
    return "AccumuloDataModel[" +table+ ']';
  }

  @Override
  public void close() {
    try {
		writer.close();
	} catch (MutationsRejectedException e) {
		e.printStackTrace();
	}

  }

  private final class UserPrefArrayRetriever implements Retriever<Long, PreferenceArray> {
    @Override
    public PreferenceArray get(Long userID) throws TasteException {

    	List<String[]> ids = new LinkedList<String[]>();

    	Scanner scan;
		try {
			scan = conn.createScanner(table, auths);
		} catch (TableNotFoundException e1) {
			throw new RuntimeException();
		}
    	scan.setRange(new Range(userID.toString()));
    	scan.fetchColumnFamily(new Text(USERS_CF));
    	for(Entry<Key, Value> e : scan)
    	{
    		ids.add(new String[]{ e.getKey().getColumnQualifier().toString(), new String(e.getValue().get())});
    	}
    	
    	PreferenceArray prefs = new GenericUserPreferenceArray(ids.size());
        prefs.setUserID(0, userID);
    	int i = 0;
    	for(String[] id : ids)
    	{
    		prefs.setItemID(i, Long.parseLong(id[0]));
            prefs.setValue(i, Float.parseFloat(id[1]));
    		++i;
    	}

    	return prefs;
    }
  }

  private final class ItemPrefArrayRetriever implements Retriever<Long, PreferenceArray> {
    @Override
    public PreferenceArray get(Long itemID) throws TasteException {
      
    	List<String[]> ids = new LinkedList<String[]>();

    	Scanner scan;
		try {
			scan = conn.createScanner(table, auths);
		} catch (TableNotFoundException e1) {
			throw new RuntimeException();
		}
    	scan.setRange(new Range(itemID.toString()));
    	scan.fetchColumnFamily(new Text(ITEMS_CF));
    	for(Entry<Key, Value> e : scan)
    	{
    		ids.add(new String[]{ e.getKey().getColumnQualifier().toString(), new String(e.getValue().get())});
    	}
    	
    	PreferenceArray prefs = new GenericItemPreferenceArray(ids.size());
        prefs.setUserID(0, itemID);
    	int i = 0;
    	for(String[] id : ids)
    	{
    		prefs.setItemID(i, Long.parseLong(id[0]));
            prefs.setValue(i, Float.parseFloat(id[1]));
    		++i;
    	}

    	return prefs;

    }
  }

  private final class UserIDsFromItemRetriever implements Retriever<Long, FastIDSet> {
    @Override
    public FastIDSet get(Long itemID) throws TasteException {
    
    	FastIDSet userIDs = new FastIDSet();
    
    	Scanner scan;
		try {
			scan = conn.createScanner(table, auths);
		} catch (TableNotFoundException e1) {
			throw new RuntimeException();
		}
    	scan.setRange(new Range(itemID.toString()));
    	scan.fetchColumnFamily(new Text(ITEMS_CF));
    	for(Entry<Key, Value> e : scan)
    	{
    		userIDs.add(Long.parseLong(e.getKey().getColumnQualifier().toString()));
    	}
    	
      return userIDs;
    }
  }

  private final class ItemIDsFromUserRetriever implements Retriever<Long, FastIDSet> {
    @Override
    public FastIDSet get(Long userID) throws TasteException {
    	FastIDSet itemIDs = new FastIDSet();
        
    	Scanner scan;
		try {
			scan = conn.createScanner(table, auths);
		} catch (TableNotFoundException e1) {
			throw new RuntimeException();
		}
    	scan.setRange(new Range(userID.toString()));
    	scan.fetchColumnFamily(new Text(USERS_CF));
    	for(Entry<Key, Value> e : scan)
    	{
    		itemIDs.add(Long.parseLong(e.getKey().getColumnQualifier().toString()));
    	}

      return itemIDs;
    }
  }

}
