package edu.sjsu.cmpe.cache.client;

/*
Nishant Jha
Student Id-010047557
lab -4
*/
public interface CacheServiceInterface {
    public String get(long key);
    public String getAsyn(long key);
    public void put(long key, String value);
    public void putAsyn(long key, String value);
}
