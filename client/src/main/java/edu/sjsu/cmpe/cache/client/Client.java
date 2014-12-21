package edu.sjsu.cmpe.cache.client;

/*
Nishant Jha
Student Id-010047557
lab -4
*/

public class Client {

    public static void main(String[] args) throws Exception {
        
        CacheServiceInterface cache = new DistributedCacheService(
                "http://localhost:3000");
        CacheServiceInterface cache1 = new DistributedCacheService(
                "http://localhost:3000","http://localhost:3001","http://localhost:3002");

     System.out.println("Starting of ClientCache");
        System.out.println("put(1=>a)");
           cache1.putAsyn(1, "a");
            System.out.println("Sleeping for 30 sec");
        Thread.sleep(30000);
           System.out.println("Update put (1=>b)");
           cache1.putAsyn(1, "b");
        System.out.println("Sleeping for 30 sec");
           Thread.sleep(30000);
            System.out.println("fetching values");
           String value = cache1.getAsyn(1);
           System.out.println("Value received "+value);
        System.out.println("Exiting ClientCache");
    }  

}
