/**
 * 
 */
package com.airvana.loadtool.persistance.connections;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

import com.airvana.loadtool.client.JobConfiguration;
import com.airvana.loadtool.commons.JobLogger;
import com.airvana.loadtool.commons.exceptions.CassandraException;
import com.airvana.loadtool.commons.exceptions.InvalidCassandraQueryException;
import com.airvana.slamd.AbstractFSMJob;
import com.datastax.driver.core.CloseFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.google.common.collect.Lists;

/**
 * <p>
 * CassandraSessionManager manages the Cassandra Session objects. This creates
 * the {@link Session} objects and stores them in the pool.
 * </p>
 * 
 * 
 * <p>
 * The maximum no. of sessions created depends upon the max connection pool
 * defined in conf/jobconfiguration.properties file.
 * </p>
 * 
 * <p>
 * The CassandraSessionManager should be instantiated by using
 * {@link #getSessionManager()}
 * </p>
 * 
 * 
 * @author akballappagari
 * 
 */
public final class CassandraSessionManager {

	/**
	 * Logger to log the info.
	 */
	private static final JobLogger LOG = JobLogger
			.getLogger(CassandraSessionManager.class);

	/**
	 * local instance of CassandraSessionManager. Only once instance created per
	 * job.
	 */
	private static CassandraSessionManager sessionManager;

	/**
	 * CassandraConnection object instance which helps to create the Cassandra
	 * {@link Session}
	 */
	private CassandraConnection connection;

	/**
	 * Total no. of sessions available per pool.
	 */
	private int MAX_AVAILABILITY_PER_POOL;

	/**
	 * A counting {@link Semaphore} used to retrieve the session from pool.
	 */
	private Semaphore sessionAvailablity;

	/**
	 * A Queue which acts a {@link Session} pool. This maintains the Session
	 * pool.
	 */
	private BlockingQueue<Session> sessionsQueue;

	/**
	 * Verify the needed variables are initialized or not. {@code initialized}
	 * will be true if initialized else false.
	 */
	private boolean initialized = false;

	private void init() {
		if (!initialized) {
			MAX_AVAILABILITY_PER_POOL = JobConfiguration.cassandraMaxPool; // 10;

			sessionAvailablity = new Semaphore(MAX_AVAILABILITY_PER_POOL, false);

			sessionsQueue = new LinkedBlockingQueue<Session>(
					MAX_AVAILABILITY_PER_POOL);
			initialized = true;
		}
	}

	/**
	 * CassandraSessionManager initialized privately to protect from outside
	 * instantiations.
	 */
	private CassandraSessionManager() {
	}

	/**
	 * Initialize the {@link CassandraSessionManager}. This should only be
	 * called by {@link AbstractFSMJob}. <b>Beware:</b> Calling
	 * {@code initSessionManager()} other than in {@link AbstractFSMJob}
	 * initialization, it may result in unexpected behavior of persistence.
	 */
	public static void initSessionManager() {
		sessionManager = new CassandraSessionManager();
	}

	/**
	 * Initialize the {@link CassandraConnection} object.
	 */
	private void initConnection() {
		if (connection == null) {
			connection = new CassandraConnection();
		}
	}

	/**
	 * Creates and returns {@link CassandraSessionManager} if not created else
	 * will return already created manager.
	 * 
	 * @return CassandraSessionManager object.
	 */
	public final static CassandraSessionManager getSessionManager() {
		sessionManager.initConnection();
		sessionManager.init();
		System.out.println("SessionManager reference is " + sessionManager);
		return sessionManager;
	}

	/**
	 * Creates and returns {@link Session}.
	 * <p>
	 * a) Will return Session from the {@link #sessionsQueue} if any available
	 * in the pool.
	 * </p>
	 * <p>
	 * b) Will create and return the session, if not available in pool and max
	 * sessions are not created.
	 * </p>
	 * <p>
	 * c) If max sessions are created and none are available in pool, will wait
	 * till session's are available in pool or throws {@link CassandraException}
	 * if failed to retrieve one.
	 * </p>
	 * 
	 * @return {@link Session} object from the pool.
	 * @throws CassandraException
	 *             if creation or retrieval of session fails.
	 */
	protected Session getSession() throws CassandraException,
			InvalidQueryException {
		try {
			LOG.info("Verifying the CassandraSession availablity........");
			sessionAvailablity.acquire();
			LOG.info("Sessions have not reached max pool. So, verifying for available Sessions .........");
			if (sessionsQueue.peek() == null) {
				LOG.info("No Session is available, so creating a new one.......");
				return connection.createSession();
			}
		} catch (InvalidQueryException iqe) {
			LOG.info("Provided keyspace not valid");
			throw new InvalidQueryException(iqe.getMessage());
		} catch (Exception e) {
			LOG.info("There was an error while trying to creating to session. Please see error log for more info");
			throw new CassandraException("Connection not available", e);
		}
		LOG.info("Sessions are available in pool. So, returning an existing Session....");
		return sessionsQueue.poll();
	}

	/**
	 * Stores the given session object to pool.
	 * 
	 * @param session
	 *            session which needs to be returned.
	 * @return true if successfully stored into pool, else false.
	 */
	protected boolean close(Session session) {
		LOG.info("Verifying the passed Session is valid..........");
		sessionAvailablity.release();
		boolean offered = sessionsQueue.offer(session);
		if (offered) {
			LOG.info("Session has been restored to pool.");
		} else {
			LOG.info("Session is not able to be restored to pool.");
		}
		return offered;
	}

	/**
	 * Shuts down all sessions in the pool. For more information refer to
	 * {@link Session#closeAsync()}
	 * 
	 * @see Session#closeAsync()
	 * @return {@link CloseFuture} list which contains the session closing
	 *         information.
	 */
	public List<CloseFuture> shutdown() {
		LOG.info("Closing all the available Sessions in pool.....");
		List<CloseFuture> closeFutureList = Lists.newArrayList();
		while (sessionsQueue.peek() != null) {
			closeFutureList.add(sessionsQueue.poll().closeAsync());
		}
		LOG.info("Close request has been made for all Sessions available in pool.....");
		LOG.info("Closing the Cassandra Cluster........");
		closeFutureList.add(sessionManager.getConnection().closeCluster());
		LOG.info("Close request has been made to the Cassandra Cluster........");
		return Collections.unmodifiableList(closeFutureList);
	}

	/**
	 * @return current {@link CassandraConnection} object.
	 */
	private final CassandraConnection getConnection() {
		return connection;
	}

	/**
	 * Verifies both the given Cassandra Node and keyspace are valid. Returns true if
	 * valid, else false.
	 * 
	 * @return true if cassandra connection is alive, else false
	 * @throws CassandraException
	 *             if any exception occurred while trying to connect to
	 *             Cassandra
	 * @throws InvalidCassandraQueryException
	 *             if any invalid query has been passed.
	 */
	public static boolean isConnectionAlive() throws CassandraException,
			InvalidCassandraQueryException {
		try {
			getSessionManager().getSession().close();
		} catch (NoHostAvailableException nhae) {
			LOG.error("Can't be connected to the host ");
			nhae.printStackTrace();
			throw new CassandraException("Can't be connected to the host ",
					nhae);
		} catch (InvalidQueryException inque) {
			LOG.error("Can't be connected as passed keyspace is invalid");
			inque.printStackTrace();
			throw new InvalidCassandraQueryException(
					"Can't be connected as passed keyspace is invalid", inque);
		}
		return true;
	}
}
