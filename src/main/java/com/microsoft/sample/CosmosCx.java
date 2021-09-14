package com.microsoft.sample;


import com.datastax.driver.core.*;

import javax.net.ssl.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.*;
import java.time.Instant;


public class CosmosCx {
	
	private Cluster cluster;
    private Configurations config = new Configurations();
    private String cassandraHost = "";
    private int cassandraPort = 10350;
    private String cassandraUsername = "";
    private String cassandraPassword = "";
    private String cassandraKeySpaceName = "";
    private String cassandraTableName = "";
    
    private File sslKeyStoreFile = null;
    private String sslKeyStorePassword = "changeit";
    


    /**
     * This method creates a Cassandra Session based on the the end-point details given in config.properties.
     * This method validates the SSL certificate based on ssl_keystore_file_path & ssl_keystore_password properties.
     * If ssl_keystore_file_path & ssl_keystore_password are not given then it uses 'cacerts' from JDK.
     * @return Session Cassandra Session
     */
    public Session getSession() {

        try {
            //Load cassandra endpoint details from config.properties
            loadCassandraConnectionDetails();

            final KeyStore keyStore = KeyStore.getInstance("JKS");
            try (final InputStream is = new FileInputStream(sslKeyStoreFile)) {
                keyStore.load(is, sslKeyStorePassword.toCharArray());
            }

            final KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory
                    .getDefaultAlgorithm());
            kmf.init(keyStore, sslKeyStorePassword.toCharArray());
            final TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory
                    .getDefaultAlgorithm());
            tmf.init(keyStore);

            // Creates a socket factory for HttpsURLConnection using JKS contents.
            final SSLContext sc = SSLContext.getInstance("TLSv1.2");
            sc.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new java.security.SecureRandom());

            JdkSSLOptions sslOptions = RemoteEndpointAwareJdkSSLOptions.builder()
                    .withSSLContext(sc)
                    .build();
            cluster = Cluster.builder()
                    .addContactPoint(cassandraHost)
                    .withPort(cassandraPort)
                    .withCredentials(cassandraUsername, cassandraPassword)
                    .withSSL(sslOptions)
                    .build();

            return cluster.connect();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public Cluster getCluster() {
        return cluster;
    }

    /**
     * Closes the cluster and Cassandra session
     */
    public void close() {
        cluster.close();
    }

    /**
     * Loads Cassandra end-point details from config.properties.
     * @throws Exception
     */
    private void loadCassandraConnectionDetails() throws Exception {
        cassandraHost = config.getProperty("cassandra_host");
        cassandraPort = Integer.parseInt(config.getProperty("cassandra_port"));
        cassandraUsername = config.getProperty("cassandra_username");
        cassandraPassword = config.getProperty("cassandra_password");
        cassandraKeySpaceName = config.getProperty("cassandra_keyspace_name");
        cassandraTableName = config.getProperty("cassandra_table_name");
        
        String ssl_keystore_file_path = config.getProperty("ssl_keystore_file_path");
        String ssl_keystore_password = config.getProperty("ssl_keystore_password");

        // If ssl_keystore_file_path, build the path using JAVA_HOME directory.
        if (ssl_keystore_file_path == null || ssl_keystore_file_path.isEmpty()) {
            String javaHomeDirectory = "/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home";
            if (javaHomeDirectory == null || javaHomeDirectory.isEmpty()) {
                throw new Exception("JAVA_HOME not set");
            }
            ssl_keystore_file_path = new StringBuilder(javaHomeDirectory).append("/jre/lib/security/cacerts").toString();
        }

        sslKeyStorePassword = "changeit";

        sslKeyStoreFile = new File(ssl_keystore_file_path);

        if (!sslKeyStoreFile.exists() || !sslKeyStoreFile.canRead()) {
            throw new Exception(String.format("Unable to access the SSL Key Store file from %s", ssl_keystore_file_path));
        }
    }
    
    public void insertData(Session session, User user) {
    	
    	StringBuilder sb = new StringBuilder("INSERT INTO ")
    		      .append(cassandraKeySpaceName+"."+cassandraTableName).append("(userid, name, email, lmdate) ")
    		      .append("VALUES (").append(user.getUserId())
    		      .append(", '").append(user.getName()).append("'")
    		      .append(", '").append(user.getEmail()).append("'")
    			  .append(", '").append(Instant.now()).append("');");

    		    String query = sb.toString();
    		    session.execute(query);
    }
    
   
    public int getMaxValue(Session session, String colName) {
    	StringBuilder sb = new StringBuilder("SELECT MAX(").append(colName)
    		  .append(") as m")
    		  .append(" FROM ")
  		      .append(cassandraKeySpaceName+"."+cassandraTableName)
  		      .append(";");
  		    String query = sb.toString();
  		    ResultSet result = session.execute(query);
  		    Row row = result.one();
  		    return row.getInt("m");
  		    
    }

}
