package edu.sjsu.cmpe.cache.client;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.mashape.unirest.http.Headers;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.async.Callback;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.mashape.unirest.request.HttpRequestWithBody;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;


/*
Nishant Jha
Student Id-010047557
lab -4
*/

public class DistributedCacheService implements CacheServiceInterface {
    private  String cacSerUrl;
    private  String[] serverUrls;
    private AtomicInteger successReadCount;
    private AtomicInteger succWCount;
    int numSer;
    public DistributedCacheService(String serverUrl) {
        this.cacSerUrl = serverUrl;
    }

      public DistributedCacheService(String...serverUrls){
        this.serverUrls=serverUrls;
        this.numSer = serverUrls.length;
    }
   

    @Override
    public void put(long key, String value) {
        HttpResponse<JsonNode> r = null;
        try {
            r = Unirest
                    .put(this.cacSerUrl + "/cache/{key}/{value}")
                    .header("accept", "application/json")
                    .routeParam("key", Long.toString(key))
                    .routeParam("value", value).asJson();
        } catch (UnirestException e) {
            System.err.println(e);
        }

        if (r.getCode() != 200) {
            System.out.println("Failed addition to cache.");
        }
    }

/*
Nishant Jha
Student Id-010047557
lab -4
*/

    @Override
    public void putAsyn(long key, String value) {

        try {
            final List<String> succServer = new ArrayList<String>();
            succWCount = new AtomicInteger(0);
            final CountDownLatch c = new CountDownLatch(numSer);
            for (int i = 0; i < numSer; i++) {
                final String currentServerURL = this.serverUrls[i];
                Future<HttpResponse<JsonNode>> future = Unirest
                        .put(currentServerURL + "/cache/{key}/{value}")
                        .header("accept", "application/json")
                        .routeParam("key", Long.toString(key))
                        .routeParam("value", value).asJsonAsync(new Callback<JsonNode>() {
                            String currentUrl = currentServerURL;
                            public void failed(UnirestException e) {
                                System.out.println("Request failed");
                                c.countDown();
                            }

                            public void completed(HttpResponse<JsonNode> r) {
                                int code = r.getCode();
                                Headers headers = r.getHeaders();
                                JsonNode body = r.getBody();
                                InputStream rawBody = r.getRawBody();
                                succWCount.incrementAndGet();
                                succServer.add(currentUrl);
                                c.countDown();
                            }

                            public void cancelled() {
                                System.out.println("Request cancelled");
                            }

                        });

            }
            c.await();

            if(numSer%2==0){
                if(succWCount.intValue()>=(numSer/2)){
                    System.out.println("Put successful on servers");
                    for(String successfulServer:succServer){
                        System.out.println(successfulServer);
                    }
                }
                else{
                    System.out.println("Values getting deleted from servers");
                    for (int i = 0; i < succServer.size(); i++) {
                        System.out.println(succServer.get(i));
                        HttpRequestWithBody r = Unirest.delete(succServer.get(i)+"/cache/{key}");
                        System.out.println(r);
                    }
                }


            }
            else{
                if(succWCount.intValue()>=((numSer/2)+1)){
                    System.out.println("Put successful on servers");
                    for(String successfulServer:succServer){
                        System.out.println(successfulServer);
                    }
                }
                else{
                    System.out.println("Values deleted from servers");
                    for (int i = 0; i < succServer.size(); i++) {
                        System.out.println(succServer.get(i));
                        HttpRequestWithBody r = Unirest.delete(succServer.get(i)+"/cache/{key}");
                        System.out.println(r);
                    }
                }


            }
        }
        catch(InterruptedException e){
            e.printStackTrace();
        }

    }


    @Override
    public String get(long key) {
        HttpResponse<JsonNode> r = null;
        try {
            r = Unirest.get(this.cacSerUrl + "/cache/{key}")
                    .header("accept", "application/json")
                    .routeParam("key", Long.toString(key)).asJson();
        } catch (UnirestException e) {
            System.err.println(e);
        }
        String value = r.getBody().getObject().getString("value");

        return value;
    }


/*
Nishant Jha
Student Id-010047557
lab -4
*/


    @Override
    public String getAsyn(long key) {

        final CountDownLatch c = new CountDownLatch(numSer);
        HttpResponse<JsonNode> r = null;
      successReadCount = new AtomicInteger(0);
        final ListMultimap<String,String> succServer = ArrayListMultimap.create();

        try {
            for (int i = 0; i < numSer; i++) {

                final String currentServerURL = this.serverUrls[i];

                Future<HttpResponse<JsonNode>> future = Unirest.get(currentServerURL + "/cache/{key}")
                        .header("accept", "application/json")
                        .routeParam("key", Long.toString(key)).asJsonAsync(new Callback<JsonNode>() {
                            String currentUrl = currentServerURL;

                            public void failed(UnirestException e) {
                                System.out.println("Request failed");
                            }

                            public void completed(HttpResponse<JsonNode> r) {
                                int code = r.getCode();
                                Headers headers = r.getHeaders();
                                JsonNode body = r.getBody();
                                InputStream rawBody = r.getRawBody();
System.out.println(r.getBody());
                                String value;
                                if (r.getBody()!=null)
                                    value = r.getBody().getObject().getString("value");
                                    else
                                        value="fault";
                                    successReadCount.incrementAndGet();
                                    succServer.put(value, currentUrl);
                                    c.countDown();
                                    ;


                            }

                            public void cancelled() {
                                System.out.println("The request has been cancelled");
                            }

                        });


            }
            c.await();
        }
        catch (InterruptedException e){
            e.printStackTrace();
        }



        List<String> sameValueServers = new ArrayList<String>();
        String successValue=null;

        for (String value : succServer.keySet()) {

            List<String> receivedServerUrls = succServer.get(value);
            if(receivedServerUrls!=null) {
                if (receivedServerUrls.size() > sameValueServers.size()) {
                    sameValueServers = receivedServerUrls;
                    successValue = value;
                }
            }
}

    List<String> faultServers = new ArrayList<String>();
        for(String srvUrl:serverUrls){
            int i=0;
            for(;i<sameValueServers.size();i++){
                if(srvUrl.equalsIgnoreCase(sameValueServers.get(i)))
                    break;
            }
            if(i>=sameValueServers.size()){
                faultServers.add(srvUrl);

            }
        }
System.out.println("Receiving successful from servers");
        for (String srvr:sameValueServers){
            System.out.println(srvr);
        }
        System.out.println("Values are repaired onservers");
        for (String srvr:faultServers){
            System.out.println(srvr);
            this.cacSerUrl=srvr;
            put(key,successValue);
        }
        return successValue;
    }



}
