package com.sidsoft.demo1;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.microsoft.azure.eventhubs.EventHubException;

public class StructJsonConverter {
	
	public JsonObject struct2Json(String structRec) {
		
		String s2=structRec.replaceAll("=","\":\"")
				.replaceAll(",","\",\"")
				.replaceAll("Struct\\{","\\{\"")
				.replaceAll("\\}","\"\\}")
				.replaceAll("\\}\"","\\}")
				.replaceAll("\"\\{","\\{") ;
		
		//System.out.println(s2);
		JsonParser parser = new JsonParser();
		JsonElement jsonTree = parser.parse(s2);
		JsonObject result =jsonTree.getAsJsonObject(); 
		
		
		//send to event hubs
		EhubPublisher ep1=new EhubPublisher();
		try {
			ep1.ehubSendor(s2);
		} catch (EventHubException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("ehubs sent");
		
		
		return result;
	}
}
