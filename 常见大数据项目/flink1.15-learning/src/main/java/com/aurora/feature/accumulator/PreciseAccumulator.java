package com.aurora.feature.accumulator;
/**
 * @descri
 *
 * @author lj.michale
 * @date 2022-04-30
 */
public class PreciseAccumulator{

    private Roaring64NavigableMap bitmap;

    public PreciseAccumulator(){
        bitmap=new Roaring64NavigableMap();
    }

    public void add(long id){
        bitmap.addLong(id);
    }

    public long getCardinality(){
        return bitmap.getLongCardinality();
    }
}