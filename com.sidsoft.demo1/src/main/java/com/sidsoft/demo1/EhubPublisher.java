package com.sidsoft.demo1;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;


public class EhubPublisher {

	public void ehubSendor(String s2) 
			throws EventHubException, ExecutionException, InterruptedException, IOException  {
		 final ConnectionStringBuilder connStr = new ConnectionStringBuilder()
				 //Endpoint=sb://ssevents.servicebus.windows.net/;SharedAccessKeyName=pokeconsumers;SharedAccessKey=rxiqASJX4BDZ8X3LNz64EkbqHExjeBefiSUdx/4dDDA=;EntityPath=pokemons"       		
				                .setNamespaceName("ssevents") // to target National clouds - use .setEndpoint(URI)
				                .setEventHubName("pokemons")
				                .setSasKeyName("pokeconsumers")
				                .setSasKey("rxiqASJX4BDZ8X3LNz64EkbqHExjeBefiSUdx/4dDDA=");

				        final Gson gson = new GsonBuilder().create();

				        // The Executor handles all asynchronous tasks and this is passed to the EventHubClient instance.
				        // This enables the user to segregate their thread pool based on the work load.
				        // This pool can then be shared across multiple EventHubClient instances.
				        // The following sample uses a single thread executor, as there is only one EventHubClient instance,
				        // handling different flavors of ingestion to Event Hubs here.
				        final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(4);

				        // Each EventHubClient instance spins up a new TCP/SSL connection, which is expensive.
				        // It is always a best practice to reuse these instances. The following sample shows this.
				        final EventHubClient ehClient = EventHubClient.createFromConnectionStringSync(connStr.toString(), executorService);

				        try {
				           

				                String payload = s2 ;
				                //PayloadEvent payload = new PayloadEvent(i);
				                byte[] payloadBytes = gson.toJson(payload).getBytes(Charset.defaultCharset());
				                EventData sendEvent = EventData.create(payloadBytes);

				                // Send - not tied to any partition
				                // Event Hubs service will round-robin the events across all Event Hubs partitions.
				                // This is the recommended & most reliable way to send to Event Hubs.
				                ehClient.sendSync(sendEvent);
				            

				          
				        } finally {
				            ehClient.closeSync();
				            executorService.shutdown();
				        }
				    }
	
	
	
}
