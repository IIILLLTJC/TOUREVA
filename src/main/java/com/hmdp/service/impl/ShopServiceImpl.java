package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Autowired
    private ShopMapper shopMapper;
    @Autowired
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
        //缓存穿透
        //Shop shop = queryWithPassThrough(id);
        Shop shop = cacheClient.queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

        //用互斥锁解决缓存击穿
        //Shop shop = queryWithMutex(id);

        //用逻辑过期解决缓存击穿
        //Shop shop = cacheClient.queryWithLogicalExpire(CACHE_SHOP_KEY, id, Shop.class, this::getById, 20L, TimeUnit.SECONDS);
        if (shop == null) {
            return Result.fail("店铺不存在");
        }

        //返回商铺信息
        return Result.ok(shop);
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    /*public Shop queryWithLogicalExpire(Long id) {
        //1.从redis查询商品缓存{"name":"lily","age":"18"}
        String key = CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2.判断缓存是否命中
        //""," ",不能进
        if (StrUtil.isBlank(shopJson)) {
            //3.不存在，返回
            return null;
        }

//        //" ",能进
//        if(StrUtil.isNotEmpty(shopJson)){
//            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
//            return Result.ok(shop);
//        }
//
//        //""," ",能进
//        if (shopJson != null) {
//            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
//            return Result.ok(shop);
//        }

        //4.命中，需要先把json反序列为对象
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);

        LocalDateTime expireTime = redisData.getExpireTime();
        //5.判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            //5.1未过期，直接返回店铺信息
            return shop;
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
                        this.saveShop2Redis(id, 30L);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        //释放锁
                        unlock(lockKey);
                    }
                });

        }

        //6.4返回过期店铺信息
        return shop;
    }*/


    /*public Shop queryWithMutex(Long id) {
        //1.从redis查询商品缓存{"name":"lily","age":"18"}
        String key = CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2.判断缓存是否命中
        //""," ",不能进
        if (StrUtil.isNotBlank(shopJson)) {
            //3.命中，返回商铺信息
            return JSONUtil.toBean(shopJson, Shop.class);
        }


        //如果redis不是"" 往下走
        if (shopJson != null) {//如果Redis 里存的是 "" 时跟下面存的一样，程序直接返回“店铺不存在”，不再查询数据库。
            return null;
        }

        //4.实现缓存重建
        //4.1获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        Shop shop;
        try {
            boolean isLock = tryLock(lockKey);
            //4.2判断是否获取成功
            if (!isLock) {
                //4.3失败，则休眠并重试
                Thread.sleep(50);
                return queryWithMutex(id);
            }

            //4.4成功,根据id查询数据库
            shop = getById(id);
            //模拟重建的延时
            Thread.sleep(200);
            //Shop shop = shopMapper.selectById(id);

            //5.判断商铺是否存在
            if (shop == null) {
                //将空值写入redis
                stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                //不存在，返回404
                return null;
            }
            //6.存在，将商铺数据写入redis
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            //7.释放互斥锁
            unlock(lockKey);
        }

        //8.返回商铺信息
        return shop;
    }*/

    /*public Shop queryWithPassThrough(Long id) {
        //1.从redis查询商品缓存{"name":"lily","age":"18"}
        String key = CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2.判断缓存是否命中
        //""," ",不能进
        if (StrUtil.isNotBlank(shopJson)) {
            //3.命中，返回商铺信息
            return JSONUtil.toBean(shopJson, Shop.class);
        }


        //如果redis不是"" 往下走
        if (shopJson != null) {//如果Redis 里存的是 "" 时跟下面存的一样，程序直接返回“店铺不存在”，不再查询数据库。
            return null;
        }

        //4.未命中,根据id查询数据库
        Shop shop = getById(id);
        //Shop shop = shopMapper.selectById(id);

        //5.判断商铺是否存在
        if (shop == null) {
            //将空值写入redis
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            //6.不存在，返回404
            return null;
        }
        //7.存在，将商铺数据写入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);

        //返回商铺信息
        return shop;
    }*/

    /*private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        //return BooleanUtil.isTrue(flag);
        return Boolean.TRUE.equals(flag);
    }

    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }

    public void saveShop2Redis(Long id, Long expireSeconds) throws InterruptedException {
        //1.查询店铺数据
        Shop shop = getById(id);
        Thread.sleep(200);
        //2.封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));

        //3.写入redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));

    }*/


    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺id不能为空");
        }
        //1.更新数据库
        updateById(shop);
        //2.删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
        //3.返回
        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        //1.判断是否需要根据坐标查询
        if (x == null || y == null) {
            //不需要坐标查询，根据类型分页查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            // 返回数据
            List<Shop> records = page.getRecords();
            return Result.ok(records);
        }
        //2.计算分页参数
        //比如current等于2 也就是第二页 那么第二页的数据也就是（2-1）*条数到2*条数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;
        String key = SHOP_GEO_KEY + typeId;
        //3.查询redis、按照距离排序、分页、结果：shopId、distance search = locations = new ArrayList<>()
        //    List<RedisGeoCommands.GeoLocation<String>> locations = new ArrayList<>(shops.size());
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo() //GEOSEARCH key bylonlat x y byradius 10 withdistance
                .search(key,
                        GeoReference.fromCoordinate(new Point(x, y)),
                        new Distance(5000), RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end));

        //4.解析出id
        //String name = results.getContent().get(0).getContent().getName();
        //List<RedisGeoCommands.GeoLocation<String>> locations = new ArrayList<>(shops.size());
        if (results == null) {
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        //4.1.截取from-end部分
        List<Long> ids = new ArrayList<>(list.size());
        Map<String, Distance> distanceMap = new HashMap<>(list.size());
        if (list.size() <= from) {
            //没有下一页了，结束
            return Result.ok(Collections.emptyList());
        }
        list.stream().skip(from).forEach(result -> {
            //4.2.获取店铺id
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            //4.3.获取店铺位置
            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr, distance);
        });

        //上面把redis中的店铺id和店铺位置存在map中 一个id对应一个位置 list再存店铺id 用来数据库查询店铺

        // 【这里是关键修改点】再次判断 ids 是否为空
        if (ids.isEmpty()) {
            return Result.ok(Collections.emptyList());
        }
        //5.根据id查询shop
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();

        //我们要店铺信息-> 信息包括店铺的位置 所以要遍历店铺 然后到map中取店铺位置 存入店铺信息中返回
        for (Shop shop : shops) {
            //map的key是店铺id private Double distance;
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }

        // 6.返回数据
        return Result.ok(shops);
    }
}
