package com.raysonfang.sqltranslator.util;


import java.util.HashMap;
import java.util.Map;

/**
* 基于Map实现内存缓存
* @author rayson.fang
* @date 2021/02/05 15:03
*/
public class MapCacheUtil {
    private static MapCacheUtil cacheUtil;
    private static Map<String,Object> cacheMap;

    private MapCacheUtil(){
        cacheMap = new HashMap<String, Object>();
    }

    public static MapCacheUtil getInstance(){
        if (cacheUtil == null){
            cacheUtil = new MapCacheUtil();
        }
        return cacheUtil;
    }

    /**
     * 添加缓存
     * @param key
     * @param obj
     */
    public void addCacheData(String key,Object obj){
        cacheMap.put(key,obj);
    }

    /**
     * 取出缓存
     * @param key
     * @return
     */
    public Object getCacheData(String key){
        return cacheMap.get(key);
    }

    /**
     * 清楚缓存
     * @param key
     */
    public void removeCacheData(String key){
        cacheMap.remove(key);
    }

    public Map cacheMap() {
        return cacheMap;
    }
}
