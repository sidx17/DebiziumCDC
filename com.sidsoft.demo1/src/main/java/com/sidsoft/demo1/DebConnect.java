package com.sidsoft.demo1;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.relational.history.MemoryDatabaseHistory;
import io.debezium.util.Clock;
import com.google.gson.Gson; 

public class DebConnect implements Runnable {
	
	  private static final Logger LOGGER = LoggerFactory.getLogger(DebConnect.class);
	  private EmbeddedEngine engine;
	  
	// Define the configuration for the embedded and MySQL connector ...
	  public Configuration config = Configuration.create()
	            /* begin engine properties */
	            .with("name", "my-sql-connector")
	            .with("connector.class", "io.debezium.connector.mysql.MySqlConnector")
	            .with("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
	            .with("offset.storage.file.filename", "D:\\experiments\\Sparkstream\\datasets\\deboffsetnew2.dat")
	            .with("offset.flush.interval.ms", 60000)
	            /* begin connector properties */
	            .with("database.hostname", "localhost")
	            .with("database.port", 3306)
	            .with("database.user", "dbmasteruser1")
	            .with("database.password", "Sid@1234")
	            .with("database.server.id", 1)
	            .with("database.server.name", "localhost")
	            .with("database.history", "io.debezium.relational.history.FileDatabaseHistory")
	            .with("database.history.file.filename", "D:\\experiments\\Sparkstream\\datasets\\dbhistorynew2.dat")
	            .with("database.whitelist", "sakila")
	            .with("table.whitelist", "sakila.city")
	            .build();

    @Override
    public void run() {
    	 engine = EmbeddedEngine.create()
                .using(config)
                .using(this.getClass().getClassLoader())
                .using(Clock.SYSTEM)
                .notifying(this::sendRecord)
                .build();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(engine);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Requesting embedded engine to shut down");
            engine.stop();
        }));

        // the submitted task keeps running, only no more new ones can be added
        executor.shutdown();

        awaitTermination(executor);

        cleanUp();

        LOGGER.info("Engine terminated");
    }
    
   //this runs for each event of the engine 
 private void sendRecord(SourceRecord record) {
        
        Schema schema = null;
        if ( null == record.keySchema() ) {
            LOGGER.error("The keySchema is missing. Something is wrong.");
            return;
        }
        // For deletes, the value node is null
        if ( null != record.valueSchema() ) {
            schema = SchemaBuilder.struct()
                    .field("key", record.keySchema())
                    .field("value", record.valueSchema())
                    .build();
        }
        else {
            schema = SchemaBuilder.struct()
                    .field("key", record.keySchema())
                    .build();
        }

        Struct message = new Struct(schema);
        message.put("key", record.key());

        if ( null != record.value() )
            message.put("value", record.value());

        System.out.println("payload is " +message.get("value"));
       
        //convert to json and send to event hubs
        StructJsonConverter sjc=new StructJsonConverter();
        System.out.println(sjc.struct2Json(message.toString()));
       }
  
  
    private void awaitTermination(ExecutorService executor) {
        try {
            while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                LOGGER.info("Waiting another 10 seconds for the embedded engine to complete");
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void cleanUp() {
  //      kinesisClient.shutdown();
    }  
	
	 public static void main(String[] args) {
	        new DebConnect().run();
	    }
	
}
