/**
 * 
 */
package com.airvana.loadtool.persistance.connections.constants;

/**
 * The default static keys used in cassandra-config.properties file.
 * 
 * @author akballappagari
 * 
 */
public final class CassandraInfoKeys {
	/**
	 * No. of nodes per datacenter count key.
	 */
	public static final String CASSANDRA_NODE_COUNT = "cassandra.nodes.count";
	/**
	 * address of node key
	 */
	public static final String CASSANDRA_NODE_NAME = "cassandra.node.0";
	/**
	 * keyspace key
	 */
	public static final String CASSANDRA_KEYSPACNE_NAME = "cassandra.keyspace";
	/**
	 * ssl key.
	 */
	public static final String CASSANDRA_SSL = "cassandra.ssl";
}
