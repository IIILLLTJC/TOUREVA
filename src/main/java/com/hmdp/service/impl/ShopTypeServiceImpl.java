package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryAll() {
        //1.redis查询数据
        String key = "cache:shopType";
        String shopTypeCache = stringRedisTemplate.opsForValue().get(key);

        //存在返回
        if(StrUtil.isNotBlank(shopTypeCache)){
            List<ShopType> shopType = JSONUtil.toList(shopTypeCache, ShopType.class);
            return Result.ok(shopType);
        }

        //不存在，数据库查询
        List<ShopType> shopTypeList = query().orderByAsc("sort").list();

        //不存在，报错
        if (shopTypeList == null || shopTypeList.isEmpty()) {
            return Result.fail("数据不存在");
        }

        //存在，存到redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shopTypeList));

        //返回数据
        return Result.ok(shopTypeList);
    }
}
