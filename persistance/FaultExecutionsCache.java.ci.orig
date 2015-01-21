/**
 * 
 */
package com.airvana.loadtool.persistance;

import java.util.List;

import com.airvana.loadtool.commons.JobLogger;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;

/**
 * Stores the queries which are failed to execute. This queries are stored if
 * there is any connection problem to Cassandra. This will not store any
 * QueryValidation failed statements.
 * 
 * <p>
 * Below is the way to initialize the FaultExecutionsCache.
 * 
 * <pre>
 * FaultExecutionsCache faultCache = FaultExecutionsCache.getFaultExecutionCache();
 * faultCache.put(&quot;serialno&quot;, &quot;value&quot;);
 * </pre>
 * 
 * </p>
 * 
 * @author akballappagari
 * 
 */
public class FaultExecutionsCache {

	/**
	 * A {@link Multimap} which stores the {@code serialno} and its failed queries.
	 */
	private final LinkedListMultimap<String, String> failedExecutionsCache = LinkedListMultimap
			.<String, String> create();
	
	private static final JobLogger LOG = JobLogger.getLogger(FaultExecutionsCache.class);

	/**
	 * instance of {@link FaultExecutionsCache}
	 */
	private static FaultExecutionsCache faultCache;

	/**
	 * Restricting the initialization
	 */
	private FaultExecutionsCache() {
	}

	// initializing FaultExecutionsCache
	static {
		faultCache = new FaultExecutionsCache();
	}

	/**
	 * Returns the already created FaultExecutionsCache instance.
	 * 
	 * @return FaultExectionsCache instance
	 */
	public static final FaultExecutionsCache getFaultExecutionCache() {
		return faultCache;
	}

	/**
	 * stores the key and value to fault cache
	 * 
	 * @param key
	 *            to the value
	 * @param value
	 *            value to be stored
	 */
	public void put(String key, String value) {
		failedExecutionsCache.put(key, value);
	}

	/**
	 * stores the key and values to fault cache
	 * 
	 * @param key
	 *            to the value
	 * @param values
	 *            values to be stored
	 */
	public void put(String key, List<String> values) {
		putAll(key, values);
	}

	/**
	 * Returns the values for the given {@code key}
	 * 
	 * @param key
	 *            for which values needs to be fetched.
	 * @return list of values for the given {@code key}
	 */
	public List<String> getValues(String key) {
		return failedExecutionsCache.get(key);
	}

	/**
	 * Removes the values for the given {@code key}
	 * 
	 * @param key
	 *            for which values to be removed.
	 */
	public void remove(String key) {
		failedExecutionsCache.removeAll(key);
	}

	/**
	 * Store key and values to fault cache.
	 * 
	 * @param key
	 *            to the values
	 * @param values
	 *            to be stored.
	 */
	public void putAll(String key, List<String> values) {
		failedExecutionsCache.putAll(key, values);
	}
	
	/**
	 * Flush the cache.
	 */
	private void flushCache() {
		Multimap<String, String> tmp = failedExecutionsCache;
		for (String key : tmp.keySet()) {
			LOG.info("Failed executions for "+key+" is being flushed... The flushed executions are: "+failedExecutionsCache.removeAll(key).toString());
		}
	}
	
	/**
	 * Flush the cache.
	 */
	public static void flush() {
		faultCache.flushCache();
	}

}
