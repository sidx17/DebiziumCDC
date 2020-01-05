package com.sidsoft.demo1;


import java.io.IOException;
import java.util.concurrent.ExecutionException;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.microsoft.azure.eventhubs.EventHubException;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.relational.history.MemoryDatabaseHistory;



public class Application {

	
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//System.out.println("Hello World");
		String s1= "Struct{key=Struct{city_id=600},value=Struct{before=Struct{city_id=600,city=Ziguinchor,country_id=88,last_update=2020-01-03T19:08:48Z},after=Struct{city_id=600,city=Ziguinchor,country_id=90,last_update=2020-01-03T19:15:10Z},source=Struct{version=0.9.5.Final,connector=mysql,name=localhost,server_id=1,ts_sec=1578078910,file=RLE0567-bin.000009,pos=2110,row=0,thread=12,db=sakila,table=city},op=u,ts_ms=1578078910576}}";
		String s2=s1.replaceAll("=","\":\"")
				.replaceAll(",","\",\"")
				.replaceAll("Struct\\{","\\{\"")
				.replaceAll("\\}","\"\\}")
				.replaceAll("\\}\"","\\}")
				.replaceAll("\"\\{","\\{") ;
		
		//System.out.println(s2);
		JsonParser parser = new JsonParser();

		JsonElement jsonTree = parser.parse(s2);
		JsonObject result =jsonTree.getAsJsonObject();
		System.out.println(result);
		
		//test the ehub connection
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
		
	
		
	}

}
