package com.hmdp.utils;


import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

@Component
@Slf4j
public class CacheClient {


    private final StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    private void set(String key, Object value, Long time, TimeUnit unit ) {//time 时间30L unit单位seconds
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit ) {
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    public <R,ID> R queryWithPassThrough(
            String keyPreFix, ID id, Class<R> type, Function<ID,R> dbFallback,Long time, TimeUnit unit) {
        //1.从redis查询商品缓存{"name":"lily","age":"18"}
        String key = keyPreFix + id;
        String json = stringRedisTemplate.opsForValue().get(key);
        //2.判断缓存是否命中
        //""," ",不能进
        if (StrUtil.isNotBlank(json)) {
            //3.命中，返回商铺信息
            return JSONUtil.toBean(json, type);
        }

        /*//" ",能进
        if(StrUtil.isNotEmpty(shopJson)){
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return Result.ok(shop);
        }

        //""," ",能进
        if (shopJson != null) {
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return Result.ok(shop);
        }*/

        //如果redis不是"" 往下走
        if (json != null) {//如果Redis 里存的是 "" 时跟下面存的一样，程序直接返回“店铺不存在”，不再查询数据库。
            return null;
        }

        //4.未命中,根据id查询数据库
        R r = dbFallback.apply(id);
        //Shop shop = shopMapper.selectById(id);

        //5.判断商铺是否存在
        if (r == null) {
            //将空值写入redis
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            //6.不存在，返回404
            return null;
        }
        //7.存在，将商铺数据写入redis
        this.set(key, r, time, unit);

        //返回商铺信息
        return r;
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public <R,ID> R queryWithLogicalExpire(String keyPreFix,ID id,Class<R> type, Function<ID,R> dbFallback,Long time, TimeUnit unit) {
        //1.从redis查询商品缓存{"name":"lily","age":"18"}
        String key = keyPreFix + id;
        String json = stringRedisTemplate.opsForValue().get(key);
        //2.判断缓存是否命中
        //""," ",不能进
        if (StrUtil.isBlank(json)) {
            //3.不存在，返回
            return null;
        }

        /*//" ",能进
        if(StrUtil.isNotEmpty(shopJson)){
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return Result.ok(shop);
        }

        //""," ",能进
        if (shopJson != null) {
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return Result.ok(shop);
        }*/

        //4.命中，需要先把json反序列为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);

        LocalDateTime expireTime = redisData.getExpireTime();
        //5.判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            //5.1未过期，直接返回店铺信息
            return r;
        }

        //5.2过期，需要缓存重建
        //6.重建缓存
        //6.1获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);

        //6.2判断是否获取锁成功
        if (isLock) {
            //6.3获取锁成功，开启独立线程，实现缓存重建

            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    //查数据库
                    R r1 = dbFallback.apply(id);
                    //写入redis
                    this.setWithLogicalExpire(key, r1, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    //释放锁
                    unlock(lockKey);
                }
            });

        }

        //6.4返回过期店铺信息
        return r;
    }

    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        //只有setIfAbsent成功才会返回ture
        //return BooleanUtil.isTrue(flag);
        return Boolean.TRUE.equals(flag);
    }

    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }
}
