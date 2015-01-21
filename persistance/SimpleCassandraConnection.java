/**
 * 
 */
package com.airvana.loadtool.persistance;

import java.util.Map;

import com.airvana.loadtool.client.DeviceModel;
import com.airvana.loadtool.client.DeviceParameter;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.LoggingRetryPolicy;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * @author akballappagari
 *
 */
public class SimpleCassandraConnection {

	private static Cluster cluster;
	
	private static Session session;
	
	static {
		createConnection();
	}
	
	private SimpleCassandraConnection() {
	}
	
	private static void createConnection() {
		Builder builder = Cluster.builder();
		builder.addContactPoints("10.193.104.24");
		PoolingOptions poolOptions = new PoolingOptions();
		poolOptions.setCoreConnectionsPerHost(HostDistance.REMOTE, 5);
		builder.withPoolingOptions(poolOptions)
				.withReconnectionPolicy(new ConstantReconnectionPolicy(100L))
				.withRetryPolicy(
						new LoggingRetryPolicy(DefaultRetryPolicy.INSTANCE));
		cluster = builder.build();
		session = cluster.connect("simulator");
	}
	
	public static SimpleCassandraConnection getConnection() {
		return new SimpleCassandraConnection();
	}
	
	public ResultSet execute(DeviceModel model) {
		//session.execute("DELETE FROM model WHERE serialno = '"+model.getSerialNumber()+"'");
		Map<String, String> modelMap = Maps.newHashMap();
		for (Map.Entry<String, DeviceParameter> entry : model.getObjectModel().entrySet()) {
			modelMap.put(entry.getKey(), entry.getValue().getStringValue());
		}
		Gson gson = new GsonBuilder().create();
		String modelString = gson.toJson(modelMap);
		modelString = modelString.replaceAll("\"", "\'");
		ResultSet result = session.execute("select * from model where serialno = '"+model.getSerialNumber()+"' ;");
		if (!result.isExhausted()) {
			result = session.execute("UPDATE model set modelObj="+modelString+" where serialno='" +model.getSerialNumber()+"'");
		} else {
			result = session.execute("INSERT INTO model(serialNo, modelObj, isAdminup) values('"+model.getSerialNumber()+"', "+modelString+", false)");
		}
		return result;
	}
	
	public ResultSet execute(String executeSt) {
		if (session == null) {
			createConnection();
		}
		return session.execute(executeSt);
	}
}
