/**
 * 
 */
package com.airvana.loadtool.persistance.connections;

import java.util.List;

import com.airvana.loadtool.commons.exceptions.CassandraException;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * <p>
 * CassandraSession is a wrapper of {@link Session} object, which acts a proxy
 * to Session object.
 * </p>
 * <p>
 * This provides all get features of <b>Session</b> with limitations on changes
 * to Session object
 * </p>
 * <p>
 * Below is the piece of snippet on how CassandraSession needs to be created and
 * used.
 * 
 * <pre>
 * Example:
 * CassandraSession session;
 * try {
 * 	session = CassandraSession.open();
 * 	// session usages
 * } catch (CassandraConnectionNotAvaialbleException ccne) {
 * 	// exception handling
 * } finally {
 * 	if (session != null) {
 * 		session.close();
 * 	}
 * }
 * </pre>
 * 
 * Any CassandraSession opened should be closed. Otherwise, this may result in
 * {@link OutOfMemoryError} or may cause unexpected behavior.
 * </p>
 * 
 * @author akballappagari
 * 
 */
public final class CassandraSession {
	
	//private static final JobLogger LOG = JobLogger.getLogger(CassandraSession.class);

	/**
	 * Session object on which CassandraSession wrapper is created.
	 */
	private List<Session> sessionList;

	/**
	 * returns true if session is returned to pool, else false
	 */
	private boolean isClosed;
	
	/**
	 * Private initialization of CassandraSession, which makes it to be
	 * initialized by only itself.
	 * 
	 * @param session
	 *            object for which CassandraSession wrapper is created.
	 */
	private CassandraSession(Session session) {
		this.sessionList = Lists.newArrayList();
		this.sessionList.add(session);
	}

	/**
	 * This creates a new CassandraSession object.
	 * 
	 * @return newly created CassandrSession object.
	 * @throws CassandraException
	 *             if any exception occurs while trying to create
	 *             {@link Session} object.
	 */
	public static CassandraSession open()
			throws CassandraException {
		return new CassandraSession(CassandraSessionManager.getSessionManager()
				.getSession());
	}

	/**
	 * @return Session object of Cassandra
	 */
	private Session getSession() {
		return this.sessionList.get(0);
	}

	/**
	 * A execute statement which will be executed on Cassandra using CQL.
	 * 
	 * @param statement
	 *            the CQL query to execute (that can be any Statement).
	 * @return {@link ResultSet} the result of the query. That result will never
	 *         be null but can be empty (and will be for any non SELECT query).
	 *         Refer to {@link ResultSet} for more info.
	 * @see #execute(String)
	 * @see #execute(String, Object...)
	 * @see Statement
	 * @see ResultSet
	 */
	public ResultSet execute(Statement statement) {
		return getSession().execute(statement);
	}

	/**
	 * A execute statement which will be executed on Cassandra using CQL.
	 * 
	 * @param query
	 *            the CQL query to execute.
	 * @return {@link ResultSet} the result of the query. That result will never
	 *         be null but can be empty (and will be for any non SELECT query).
	 *         Refer to {@link ResultSet} for more info.
	 * @see #execute(Statement)
	 * @see #execute(String, Object...)
	 * @see ResultSet
	 */
	public ResultSet execute(String query) {
		return getSession().execute(query);
	}

	/**
	 * Executes the provided query using the provided value.
	 * 
	 * @param query
	 *            the CQL query to execute.
	 * @param values
	 *            values required for the execution of query.
	 * @return the result of the query. That result will never be null but can
	 *         be empty (and will be for any non SELECT query).
	 */
	public ResultSet execute(String query, Object... values) {
		return getSession().execute(query, values);
	}

	/**
	 * Executes the provided query asynchronously.
	 * 
	 * This method does not block. It returns as soon as the query has been
	 * passed to the underlying network stack. In particular, returning from
	 * this method does not guarantee that the query is valid or has even been
	 * submitted to a live node. Any exception pertaining to the failure of the
	 * query will be thrown when accessing the {@link ResultSetFuture}.
	 * <p>
	 * Note that for queries that doesn't return a result (INSERT, UPDATE and
	 * DELETE), you will need to access the ResultSetFuture (that is call one of
	 * its get method to make sure the query was successful.
	 * 
	 * 
	 * @param statement
	 *            the CQL query to execute (that can be any Statement).
	 * @return a future on the result of the query.
	 */
	public ResultSetFuture executeAsync(Statement statement) {
		return getSession().executeAsync(statement);
	}

	/**
	 * Executes the provided query asynchronously using the provided values.
	 * 
	 * @param query
	 *            the CQL query to execute.
	 * 
	 * @return a future on the result of the query.
	 */
	public ResultSetFuture executeAsync(String query) {
		return getSession().executeAsync(query);
	}

	/**
	 * Executes the provided query asynchronously using the provided values.
	 * 
	 * @param query
	 *            the CQL query to execute.
	 * 
	 * @param values
	 *            values required for the execution of query.
	 * @return a future on the result of the query.
	 */
	public ResultSetFuture executeAsync(String query, Object... values) {
		return getSession().executeAsync(query, values);
	}

	/**
	 * Prepares the provided query.
	 * <p>
	 * This method is essentially a shortcut for
	 * {@code prepare(statement.getQueryString())}, but note that the resulting
	 * {@code PreparedStatement} will inherit the query properties set on
	 * {@code statement}. Concretely, this means that in the following code:
	 * 
	 * <pre>
	 * RegularStatement toPrepare = new SimpleStatement(&quot;SELECT * FROM test WHERE k=?&quot;)
	 * 		.setConsistencyLevel(ConsistencyLevel.QUORUM);
	 * PreparedStatement prepared = session.prepare(toPrepare);
	 * session.execute(prepared.bind(&quot;someValue&quot;));
	 * </pre>
	 * 
	 * the final execution will be performed with Quorum consistency.
	 * <p>
	 * Please note that if the same CQL statement is prepared more than once,
	 * all calls to this method will return the same {@code PreparedStatement}
	 * object but the method will still apply the properties of the prepared
	 * {@code Statement} to this object.
	 * 
	 * @param statement
	 *            the CQL query to execute (that can be any Statement).
	 * @return the prepared statement corresponding to statement.
	 */
	public PreparedStatement prepare(RegularStatement statement) {
		return getSession().prepare(statement);
	}

	/**
	 * Prepares the provided query string.
	 * 
	 * @param query
	 *            the CQL query string to prepare
	 * @return the prepared statement corresponding to query.
	 */
	public PreparedStatement prepare(String query) {
		return getSession().prepare(query);
	}

	/**
	 * Prepares the provided query asynchronously.
	 * <p>
	 * This method is essentially a shortcut for
	 * {@code prepareAsync(statement.getQueryString())}, but with the additional
	 * effect that the resulting {@code PreparedStatement} will inherit the
	 * query properties set on {@code statement}.
	 * <p>
	 * Please note that if the same CQL statement is prepared more than once,
	 * all calls to this method will return the same {@code PreparedStatement}
	 * object but the method will still apply the properties of the prepared
	 * {@code Statement} to this object.
	 * 
	 * @param statement
	 *            the statement to prepare
	 * @return a future on the prepared statement corresponding to
	 *         {@code statement}.
	 * 
	 * @see Session#prepare(RegularStatement)
	 */
	public ListenableFuture<PreparedStatement> prepareAsync(
			RegularStatement statement) {
		return getSession().prepareAsync(statement);
	}

	/**
	 * Prepares the provided query string asynchronously.
	 * <p>
	 * This method is equivalent to {@link #prepare(String)} except that it does
	 * not block but return a future instead. Any error during preparation will
	 * be thrown when accessing the future, not by this method itself.
	 * 
	 * @param query
	 *            the CQL query string to prepare
	 * @return a future on the prepared statement corresponding to {@code query}
	 *         .
	 */
	public ListenableFuture<PreparedStatement> prepareAsync(String query) {
		return getSession().prepareAsync(query);
	}

	/**
	 * Closes the current CassandraSession object. Once it is closed,
	 * CassandraSession is no longer available for execution.
	 * 
	 * @return true if closed successfully, else false.
	 */
	public boolean close() {
		if (getSession() != null) {
			isClosed = CassandraSessionManager.getSessionManager().close(
					this.sessionList.remove(0));
		}
		return isClosed;
	}

	/**
	 * Provides information whether the current CassandraSession is closed or
	 * not.
	 * 
	 * @return true if close, else false.
	 * 
	 */
	public boolean isClosed() {
		return isClosed;
	}

	/**
	 * Returns the Cluster object this session is part of.
	 * 
	 * @return the Cluster object this session is part of.
	 */
	public Cluster getCluster() {
		return getSession().getCluster();
	}

	/**
	 * The keyspace to which this Session is currently logged in, if any.
	 * <p>
	 * This correspond to the name passed to {@link Cluster#connect(String)}, or
	 * to the last keyspace logged into through a "USE" CQL query if one was
	 * used.
	 * </p>
	 * 
	 * @return the name of the keyspace to which this Session is currently
	 *         logged in, or {@code null} if the session is logged to no
	 *         keyspace.
	 */
	public String getLoggedKeyspace() {
		return getSession().getLoggedKeyspace();
	}
}
