# Column family(CF)
- `default` to hold user values
- `write` to record changes
- `lock` to store locks

# MVCC(version sort by ts)
> Keys are encoded in such a way that an ascending order of encoded keys orders first by user key (ascending),   
> then by timestamp (descending).

## same userkey(uk),difference ts,check sort

key was encode by `uk` and `ts`;
ts1 < ts2 < ts3
sequence in storage:
`uk-ts3` `uk-ts2` `uk-ts1` , mean `uk-ts3`was insert lastest;and `uk-ts1` was insert first;  
search `uk-ts` 是要查找早于或者等于ts的key。  
所以，假设 `ts1 < t2 < ts < ts3`，那么，查找返回的是`uk-ts2`；  
而，在storage中的排序是 `uk-ts3` `uk-ts` `uk-ts2` `uk-ts1`;  
所以，查找最近(ts)的插入值，那么直接seek(`uk-ts`)就直接返回正确的值了；  
如果要查找最早插入的值，那么需要seek之后，查找最后一条记录，所以只需要另ts=0就可以了（0比所有时间小，所以在storage在最后面）

# mvcc

## 写请求
&emsp;&emsp;假设，一个写请求，有n组key/value，分别是`(key_0,value_0) ~ key_n,valu_n0)`,选`key_0`为`primary-key`。

### 1、preWrite
- 1.1、校验
  - lock校验：检查每一个key是否有key；如果有，那么就返回失败
- 1.2、写`cf_lock`
  - 将每一个key的lock的`primary-key`都赋值为`key_0`；
- 1.3、写`cf_default`
  - 将每一个key/value写入。


### 2、write（commit）
- 2.1、校验
  - 校验lock：是否存在，并且挣锁
- 2.2、写`cf_write`
- 2.3、删除`cf_lock`

### 3、异常
- 






