/**
 * @Description:
 * @date:   2018年12月4日 上午9:26:36   
 * @version V1.0
 */
package redis.clients.jedis;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.util.Set;

import redis.clients.jedis.commands.BinaryJedisCommands;
import redis.clients.jedis.commands.JedisCommands;
import redis.clients.jedis.params.GeoRadiusParam;
import redis.clients.jedis.params.SetParams;
import redis.clients.jedis.params.ZAddParams;
import redis.clients.jedis.params.ZIncrByParams;
import redis.clients.jedis.util.Hashing;
import redis.clients.jedis.util.Pool;
import redis.clients.jedis.util.Sharded;

/**
 * @Description:
 * 
 * @author: 李光强
 * @date: 2018年12月4日 上午9:26:36
 */
public class ShardedSentinel extends Sharded<JedisSentinelPool, SentinelShareInfo>
		implements JedisCommands, Closeable, BinaryJedisCommands {

	protected ShardedSentinelPool dataSource = null;

	public ShardedSentinel(List<SentinelShareInfo> shards, Hashing algo, Pattern tagPattern) {
		super(shards, algo, tagPattern);
	}

	public ShardedSentinel(List<SentinelShareInfo> shards, Hashing algo) {
		super(shards, algo);
	}

	public ShardedSentinel(List<SentinelShareInfo> shards, Pattern tagPattern) {
		super(shards, tagPattern);
	}

	public ShardedSentinel(List<SentinelShareInfo> shards) {
		super(shards);
	}

	@Override
	public void close() {
		if (dataSource != null) {
			dataSource.returnResource(this);
		}
	}

	public void setDataSource(ShardedSentinelPool shardedJedisPool) {
		this.dataSource = shardedJedisPool;
	}

	@Override
	public String set(final String key, final String value) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.set(key, value);
		} finally {
			j.close();
		}
	}

	@Override
	public String set(final String key, final String value, SetParams params) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.set(key, value, params);
		} finally {
			j.close();
		}
	}

	@Override
	public String get(final String key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.get(key);
		} finally {
			j.close();
		}
	}

	@Override
	public String echo(final String string) {
		// Jedis j = getShard(string);
		JedisSentinelPool p = getShard(string);
		Jedis j = p.getResource();
		try {
			return j.echo(string);
		} finally {
			j.close();
		}
	}

	@Override
	public Boolean exists(final String key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.exists(key);
		} finally {
			j.close();
		}
	}

	@Override
	public String type(final String key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.type(key);
		} finally {
			j.close();
		}
	}

	@Override
	public byte[] dump(final String key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.dump(key);
		} finally {
			j.close();
		}
	}

	@Override
	public String restore(final String key, final int ttl, final byte[] serializedValue) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.restore(key, ttl, serializedValue);
		} finally {
			j.close();
		}
	}

	@Override
	public String restoreReplace(final String key, final int ttl, final byte[] serializedValue) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.restoreReplace(key, ttl, serializedValue);
		} finally {
			j.close();
		}
	}

	@Override
	public Long expire(final String key, final int seconds) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.expire(key, seconds);
		} finally {
			j.close();
		}
	}

	@Override
	public Long pexpire(final String key, final long milliseconds) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.pexpire(key, milliseconds);
		} finally {
			j.close();
		}
	}

	@Override
	public Long expireAt(final String key, final long unixTime) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.expireAt(key, unixTime);
		} finally {
			j.close();
		}
	}

	@Override
	public Long pexpireAt(final String key, final long millisecondsTimestamp) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.pexpireAt(key, millisecondsTimestamp);
		} finally {
			j.close();
		}
	}

	@Override
	public Long ttl(final String key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.ttl(key);
		} finally {
			j.close();
		}
	}

	@Override
	public Long pttl(final String key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.pttl(key);
		} finally {
			j.close();
		}
	}

	@Override
	public Boolean setbit(final String key, final long offset, boolean value) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.setbit(key, offset, value);
		} finally {
			j.close();
		}
	}

	@Override
	public Boolean setbit(final String key, final long offset, final String value) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.setbit(key, offset, value);
		} finally {
			j.close();
		}
	}

	@Override
	public Boolean getbit(final String key, final long offset) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.getbit(key, offset);
		} finally {
			j.close();
		}
	}

	@Override
	public Long setrange(final String key, final long offset, final String value) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.setrange(key, offset, value);
		} finally {
			j.close();
		}
	}

	@Override
	public String getrange(final String key, final long startOffset, final long endOffset) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.getrange(key, startOffset, endOffset);
		} finally {
			j.close();
		}
	}

	@Override
	public String getSet(final String key, final String value) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.getSet(key, value);
		} finally {
			j.close();
		}
	}

	@Override
	public Long setnx(final String key, final String value) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.setnx(key, value);
		} finally {
			j.close();
		}
	}

	@Override
	public String setex(final String key, final int seconds, final String value) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.setex(key, seconds, value);
		} finally {
			j.close();
		}
	}

	@Override
	public String psetex(final String key, final long milliseconds, final String value) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.psetex(key, milliseconds, value);
		} finally {
			j.close();
		}
	}

	public List<String> blpop(final String arg) {
		// Jedis j = getShard(arg);
		JedisSentinelPool p = getShard(arg);
		Jedis j = p.getResource();
		try {
			return j.blpop(arg);
		} finally {
			j.close();
		}
	}

	@Override
	public List<String> blpop(final int timeout, final String key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.blpop(timeout, key);
		} finally {
			j.close();
		}
	}

	public List<String> brpop(final String arg) {
		// Jedis j = getShard(arg);
		JedisSentinelPool p = getShard(arg);
		Jedis j = p.getResource();
		try {
			return j.brpop(arg);
		} finally {
			j.close();
		}
	}

	@Override
	public List<String> brpop(final int timeout, final String key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.brpop(timeout, key);
		} finally {
			j.close();
		}
	}

	@Override
	public Long decrBy(final String key, final long decrement) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.decrBy(key, decrement);
		} finally {
			j.close();
		}
	}

	@Override
	public Long decr(final String key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.decr(key);
		} finally {
			j.close();
		}
	}

	@Override
	public Long incrBy(final String key, final long increment) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.incrBy(key, increment);
		} finally {
			j.close();
		}
	}

	@Override
	public Double incrByFloat(final String key, final double increment) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.incrByFloat(key, increment);
		} finally {
			j.close();
		}
	}

	@Override
	public Long incr(final String key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.incr(key);
		} finally {
			j.close();
		}
	}

	@Override
	public Long append(final String key, final String value) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.append(key, value);
		} finally {
			j.close();
		}
	}

	@Override
	public String substr(final String key, final int start, final int end) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.substr(key, start, end);
		} finally {
			j.close();
		}
	}

	@Override
	public Long hset(final String key, final String field, final String value) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.hset(key, field, value);
		} finally {
			j.close();
		}
	}

	@Override
	public Long hset(final String key, final Map<String, String> hash) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.hset(key, hash);
		} finally {
			j.close();
		}
	}

	@Override
	public String hget(final String key, final String field) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.hget(key, field);
		} finally {
			j.close();
		}
	}

	@Override
	public Long hsetnx(final String key, final String field, final String value) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.hsetnx(key, field, value);
		} finally {
			j.close();
		}
	}

	@Override
	public String hmset(final String key, final Map<String, String> hash) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.hmset(key, hash);
		} finally {
			j.close();
		}
	}

	@Override
	public List<String> hmget(final String key, String... fields) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.hmget(key, fields);
		} finally {
			j.close();
		}
	}

	@Override
	public Long hincrBy(final String key, final String field, final long value) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.hincrBy(key, field, value);
		} finally {
			j.close();
		}
	}

	@Override
	public Double hincrByFloat(final String key, final String field, final double value) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.hincrByFloat(key, field, value);
		} finally {
			j.close();
		}
	}

	@Override
	public Boolean hexists(final String key, final String field) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.hexists(key, field);
		} finally {
			j.close();
		}
	}

	@Override
	public Long del(final String key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.del(key);
		} finally {
			j.close();
		}
	}

	@Override
	public Long unlink(final String key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.unlink(key);
		} finally {
			j.close();
		}
	}

	@Override
	public Long hdel(final String key, String... fields) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.hdel(key, fields);
		} finally {
			j.close();
		}
	}

	@Override
	public Long hlen(final String key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.hlen(key);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<String> hkeys(final String key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.hkeys(key);
		} finally {
			j.close();
		}
	}

	@Override
	public List<String> hvals(final String key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.hvals(key);
		} finally {
			j.close();
		}
	}

	@Override
	public Map<String, String> hgetAll(final String key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.hgetAll(key);
		} finally {
			j.close();
		}
	}

	@Override
	public Long rpush(final String key, String... strings) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.rpush(key, strings);
		} finally {
			j.close();
		}
	}

	@Override
	public Long lpush(final String key, String... strings) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.lpush(key, strings);
		} finally {
			j.close();
		}
	}

	@Override
	public Long lpushx(final String key, String... string) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.lpushx(key, string);
		} finally {
			j.close();
		}
	}

	@Override
	public Long strlen(final String key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.strlen(key);
		} finally {
			j.close();
		}
	}

	@Override
	public Long move(final String key, final int dbIndex) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.move(key, dbIndex);
		} finally {
			j.close();
		}
	}

	@Override
	public Long rpushx(final String key, String... string) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.rpushx(key, string);
		} finally {
			j.close();
		}
	}

	@Override
	public Long persist(final String key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.persist(key);
		} finally {
			j.close();
		}
	}

	@Override
	public Long llen(final String key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.llen(key);
		} finally {
			j.close();
		}
	}

	@Override
	public List<String> lrange(final String key, final long start, final long stop) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.lrange(key, start, stop);
		} finally {
			j.close();
		}
	}

	@Override
	public String ltrim(final String key, final long start, final long stop) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.ltrim(key, start, stop);
		} finally {
			j.close();
		}
	}

	@Override
	public String lindex(final String key, final long index) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.lindex(key, index);
		} finally {
			j.close();
		}
	}

	@Override
	public String lset(final String key, final long index, final String value) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.lset(key, index, value);
		} finally {
			j.close();
		}
	}

	@Override
	public Long lrem(final String key, final long count, final String value) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.lrem(key, count, value);
		} finally {
			j.close();
		}
	}

	@Override
	public String lpop(final String key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.lpop(key);
		} finally {
			j.close();
		}
	}

	@Override
	public String rpop(final String key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.rpop(key);
		} finally {
			j.close();
		}
	}

	@Override
	public Long sadd(final String key, String... members) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.sadd(key, members);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<String> smembers(final String key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.smembers(key);
		} finally {
			j.close();
		}
	}

	@Override
	public Long srem(final String key, String... members) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.srem(key, members);
		} finally {
			j.close();
		}
	}

	@Override
	public String spop(final String key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.spop(key);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<String> spop(final String key, final long count) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.spop(key, count);
		} finally {
			j.close();
		}
	}

	@Override
	public Long scard(final String key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.scard(key);
		} finally {
			j.close();
		}
	}

	@Override
	public Boolean sismember(final String key, final String member) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.sismember(key, member);
		} finally {
			j.close();
		}
	}

	@Override
	public String srandmember(final String key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.srandmember(key);
		} finally {
			j.close();
		}
	}

	@Override
	public List<String> srandmember(final String key, final int count) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.srandmember(key, count);
		} finally {
			j.close();
		}
	}

	@Override
	public Long zadd(final String key, final double score, final String member) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zadd(key, score, member);
		} finally {
			j.close();
		}
	}

	@Override
	public Long zadd(final String key, final double score, final String member, final ZAddParams params) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zadd(key, score, member, params);
		} finally {
			j.close();
		}
	}

	@Override
	public Long zadd(final String key, final Map<String, Double> scoreMembers) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zadd(key, scoreMembers);
		} finally {
			j.close();
		}
	}

	@Override
	public Long zadd(final String key, final Map<String, Double> scoreMembers, final ZAddParams params) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zadd(key, scoreMembers, params);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<String> zrange(final String key, final long start, final long stop) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrange(key, start, stop);
		} finally {
			j.close();
		}
	}

	@Override
	public Long zrem(final String key, String... members) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrem(key, members);
		} finally {
			j.close();
		}
	}

	@Override
	public Double zincrby(final String key, final double increment, final String member) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zincrby(key, increment, member);
		} finally {
			j.close();
		}
	}

	@Override
	public Double zincrby(final String key, final double increment, final String member, ZIncrByParams params) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zincrby(key, increment, member, params);
		} finally {
			j.close();
		}
	}

	@Override
	public Long zrank(final String key, final String member) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrank(key, member);
		} finally {
			j.close();
		}
	}

	@Override
	public Long zrevrank(final String key, final String member) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrevrank(key, member);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<String> zrevrange(final String key, final long start, final long stop) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrevrange(key, start, stop);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<Tuple> zrangeWithScores(final String key, final long start, final long stop) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrangeWithScores(key, start, stop);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<Tuple> zrevrangeWithScores(final String key, final long start, final long stop) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrevrangeWithScores(key, start, stop);
		} finally {
			j.close();
		}
	}

	@Override
	public Long zcard(final String key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zcard(key);
		} finally {
			j.close();
		}
	}

	@Override
	public Double zscore(final String key, final String member) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zscore(key, member);
		} finally {
			j.close();
		}
	}

	@Override
	public List<String> sort(final String key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.sort(key);
		} finally {
			j.close();
		}
	}

	@Override
	public List<String> sort(final String key, final SortingParams sortingParameters) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.sort(key, sortingParameters);
		} finally {
			j.close();
		}
	}

	@Override
	public Long zcount(final String key, final double min, final double max) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zcount(key, min, max);
		} finally {
			j.close();
		}
	}

	@Override
	public Long zcount(final String key, final String min, final String max) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zcount(key, min, max);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<String> zrangeByScore(final String key, final double min, final double max) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrangeByScore(key, min, max);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<String> zrevrangeByScore(final String key, final double max, final double min) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrevrangeByScore(key, max, min);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<String> zrangeByScore(final String key, final double min, final double max, final int offset,
			final int count) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrangeByScore(key, min, max, offset, count);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<String> zrevrangeByScore(final String key, final double max, final double min, final int offset,
			final int count) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrevrangeByScore(key, max, min, offset, count);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<Tuple> zrangeByScoreWithScores(final String key, final double min, final double max) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrangeByScoreWithScores(key, min, max);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<Tuple> zrevrangeByScoreWithScores(final String key, final double max, final double min) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrevrangeByScoreWithScores(key, max, min);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<Tuple> zrangeByScoreWithScores(final String key, final double min, final double max, final int offset,
			final int count) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrangeByScoreWithScores(key, min, max, offset, count);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<Tuple> zrevrangeByScoreWithScores(final String key, final double max, final double min, final int offset,
			final int count) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrevrangeByScoreWithScores(key, max, min, offset, count);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<String> zrangeByScore(final String key, final String min, final String max) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrangeByScore(key, min, max);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<String> zrevrangeByScore(final String key, final String max, final String min) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrevrangeByScore(key, max, min);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<String> zrangeByScore(final String key, final String min, final String max, final int offset,
			final int count) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrangeByScore(key, min, max, offset, count);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<String> zrevrangeByScore(final String key, final String max, final String min, final int offset,
			final int count) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrevrangeByScore(key, max, min, offset, count);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<Tuple> zrangeByScoreWithScores(final String key, final String min, final String max) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrangeByScoreWithScores(key, min, max);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<Tuple> zrevrangeByScoreWithScores(final String key, final String max, final String min) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrevrangeByScoreWithScores(key, max, min);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<Tuple> zrangeByScoreWithScores(final String key, final String min, final String max, final int offset,
			final int count) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrangeByScoreWithScores(key, min, max, offset, count);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<Tuple> zrevrangeByScoreWithScores(final String key, final String max, final String min, final int offset,
			final int count) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrevrangeByScoreWithScores(key, max, min, offset, count);
		} finally {
			j.close();
		}
	}

	@Override
	public Long zremrangeByRank(final String key, final long start, final long stop) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zremrangeByRank(key, start, stop);
		} finally {
			j.close();
		}
	}

	@Override
	public Long zremrangeByScore(final String key, final double min, final double max) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zremrangeByScore(key, min, max);
		} finally {
			j.close();
		}
	}

	@Override
	public Long zremrangeByScore(final String key, final String min, final String max) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zremrangeByScore(key, min, max);
		} finally {
			j.close();
		}
	}

	@Override
	public Long zlexcount(final String key, final String min, final String max) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zlexcount(key, min, max);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<String> zrangeByLex(final String key, final String min, final String max) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrangeByLex(key, min, max);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<String> zrangeByLex(final String key, final String min, final String max, final int offset,
			final int count) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrangeByLex(key, min, max, offset, count);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<String> zrevrangeByLex(final String key, final String max, final String min) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrevrangeByLex(key, max, min);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<String> zrevrangeByLex(final String key, final String max, final String min, final int offset,
			final int count) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrevrangeByLex(key, max, min, offset, count);
		} finally {
			j.close();
		}
	}

	@Override
	public Long zremrangeByLex(final String key, final String min, final String max) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zremrangeByLex(key, min, max);
		} finally {
			j.close();
		}
	}

	@Override
	public Long linsert(final String key, final ListPosition where, final String pivot, final String value) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.linsert(key, where, pivot, value);
		} finally {
			j.close();
		}
	}

	@Override
	public Long bitcount(final String key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.bitcount(key);
		} finally {
			j.close();
		}
	}

	@Override
	public Long bitcount(final String key, final long start, final long end) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.bitcount(key, start, end);
		} finally {
			j.close();
		}
	}

	@Override
	public Long bitpos(final String key, final boolean value) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.bitpos(key, value);
		} finally {
			j.close();
		}
	}

	@Override
	public Long bitpos(final String key, boolean value, final BitPosParams params) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.bitpos(key, value, params);
		} finally {
			j.close();
		}
	}

	@Override
	public ScanResult<Entry<String, String>> hscan(final String key, final String cursor) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.hscan(key, cursor);
		} finally {
			j.close();
		}
	}

	@Override
	public ScanResult<Entry<String, String>> hscan(final String key, final String cursor, final ScanParams params) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.hscan(key, cursor, params);
		} finally {
			j.close();
		}
	}

	@Override
	public ScanResult<String> sscan(final String key, final String cursor) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.sscan(key, cursor);
		} finally {
			j.close();
		}
	}

	@Override
	public ScanResult<Tuple> zscan(final String key, final String cursor) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zscan(key, cursor);
		} finally {
			j.close();
		}
	}

	@Override
	public ScanResult<Tuple> zscan(final String key, final String cursor, final ScanParams params) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zscan(key, cursor, params);
		} finally {
			j.close();
		}
	}

	@Override
	public ScanResult<String> sscan(final String key, final String cursor, final ScanParams params) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.sscan(key, cursor, params);
		} finally {
			j.close();
		}
	}

	@Override
	public Long pfadd(final String key, final String... elements) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.pfadd(key, elements);
		} finally {
			j.close();
		}
	}

	@Override
	public long pfcount(final String key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.pfcount(key);
		} finally {
			j.close();
		}
	}

	@Override
	public Long touch(final String key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.touch(key);
		} finally {
			j.close();
		}
	}

	@Override
	public Long geoadd(final String key, final double longitude, final double latitude, final String member) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.geoadd(key, longitude, latitude, member);
		} finally {
			j.close();
		}
	}

	@Override
	public Long geoadd(final String key, final Map<String, GeoCoordinate> memberCoordinateMap) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.geoadd(key, memberCoordinateMap);
		} finally {
			j.close();
		}
	}

	@Override
	public Double geodist(final String key, final String member1, final String member2) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.geodist(key, member1, member2);
		} finally {
			j.close();
		}
	}

	@Override
	public Double geodist(final String key, final String member1, final String member2, final GeoUnit unit) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.geodist(key, member1, member2, unit);
		} finally {
			j.close();
		}
	}

	@Override
	public List<String> geohash(final String key, final String... members) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.geohash(key, members);
		} finally {
			j.close();
		}
	}

	@Override
	public List<GeoCoordinate> geopos(final String key, final String... members) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.geopos(key, members);
		} finally {
			j.close();
		}
	}

	@Override
	public List<GeoRadiusResponse> georadius(final String key, final double longitude, final double latitude,
			final double radius, final GeoUnit unit) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.georadius(key, longitude, latitude, radius, unit);
		} finally {
			j.close();
		}
	}

	@Override
	public List<GeoRadiusResponse> georadius(final String key, final double longitude, final double latitude,
			final double radius, final GeoUnit unit, final GeoRadiusParam param) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.georadius(key, longitude, latitude, radius, unit, param);
		} finally {
			j.close();
		}
	}

	@Override
	public List<GeoRadiusResponse> georadiusByMember(final String key, final String member, final double radius,
			final GeoUnit unit) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.georadiusByMember(key, member, radius, unit);
		} finally {
			j.close();
		}
	}

	@Override
	public List<GeoRadiusResponse> georadiusByMember(final String key, final String member, final double radius,
			final GeoUnit unit, final GeoRadiusParam param) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.georadiusByMember(key, member, radius, unit, param);
		} finally {
			j.close();
		}
	}

	@Override
	public List<Long> bitfield(final String key, final String... arguments) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.bitfield(key, arguments);
		} finally {
			j.close();
		}
	}

	@Override
	public Long hstrlen(final String key, final String field) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.hstrlen(key, field);
		} finally {
			j.close();
		}
	}

	@Override
	public String set(final byte[] key, final byte[] value) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.set(key, value);
		} finally {
			j.close();
		}
	}

	@Override
	public String set(final byte[] key, final byte[] value, SetParams params) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.set(key, value, params);
		} finally {
			j.close();
		}
	}

	@Override
	public byte[] get(final byte[] key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.get(key);
		} finally {
			j.close();
		}
	}

	@Override
	public Boolean exists(final byte[] key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.exists(key);
		} finally {
			j.close();
		}
	}

	@Override
	public String type(final byte[] key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.type(key);
		} finally {
			j.close();
		}
	}

	@Override
	public byte[] dump(final byte[] key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.dump(key);
		} finally {
			j.close();
		}
	}

	@Override
	public String restore(final byte[] key, final int ttl, final byte[] serializedValue) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.restore(key, ttl, serializedValue);
		} finally {
			j.close();
		}
	}

	@Override
	public String restoreReplace(final byte[] key, final int ttl, final byte[] serializedValue) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.restoreReplace(key, ttl, serializedValue);
		} finally {
			j.close();
		}
	}

	@Override
	public Long expire(final byte[] key, final int seconds) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.expire(key, seconds);
		} finally {
			j.close();
		}
	}

	@Override
	public Long pexpire(final byte[] key, final long milliseconds) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.pexpire(key, milliseconds);
		} finally {
			j.close();
		}
	}

	@Override
	public Long expireAt(final byte[] key, final long unixTime) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.expireAt(key, unixTime);
		} finally {
			j.close();
		}
	}

	@Override
	public Long pexpireAt(final byte[] key, final long millisecondsTimestamp) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.pexpireAt(key, millisecondsTimestamp);
		} finally {
			j.close();
		}
	}

	@Override
	public Long ttl(final byte[] key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.ttl(key);
		} finally {
			j.close();
		}
	}

	@Override
	public Long pttl(final byte[] key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.pttl(key);
		} finally {
			j.close();
		}
	}

	@Override
	public Long touch(final byte[] key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.touch(key);
		} finally {
			j.close();
		}
	}

	@Override
	public byte[] getSet(final byte[] key, final byte[] value) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.getSet(key, value);
		} finally {
			j.close();
		}
	}

	@Override
	public Long setnx(final byte[] key, final byte[] value) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.setnx(key, value);
		} finally {
			j.close();
		}
	}

	@Override
	public String setex(final byte[] key, final int seconds, final byte[] value) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.setex(key, seconds, value);
		} finally {
			j.close();
		}
	}

	@Override
	public String psetex(final byte[] key, final long milliseconds, final byte[] value) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.psetex(key, milliseconds, value);
		} finally {
			j.close();
		}
	}

	@Override
	public Long decrBy(final byte[] key, final long decrement) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.decrBy(key, decrement);
		} finally {
			j.close();
		}
	}

	@Override
	public Long decr(final byte[] key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.decr(key);
		} finally {
			j.close();
		}
	}

	@Override
	public Long del(final byte[] key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.del(key);
		} finally {
			j.close();
		}
	}

	@Override
	public Long unlink(final byte[] key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.unlink(key);
		} finally {
			j.close();
		}
	}

	@Override
	public Long incrBy(final byte[] key, final long increment) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.incrBy(key, increment);
		} finally {
			j.close();
		}
	}

	@Override
	public Double incrByFloat(final byte[] key, final double increment) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.incrByFloat(key, increment);
		} finally {
			j.close();
		}
	}

	@Override
	public Long incr(final byte[] key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.incr(key);
		} finally {
			j.close();
		}
	}

	@Override
	public Long append(final byte[] key, final byte[] value) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.append(key, value);
		} finally {
			j.close();
		}
	}

	@Override
	public byte[] substr(final byte[] key, final int start, final int end) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.substr(key, start, end);
		} finally {
			j.close();
		}
	}

	@Override
	public Long hset(final byte[] key, final byte[] field, final byte[] value) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.hset(key, field, value);
		} finally {
			j.close();
		}
	}

	@Override
	public Long hset(final byte[] key, final Map<byte[], byte[]> hash) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.hset(key, hash);
		} finally {
			j.close();
		}
	}

	@Override
	public byte[] hget(final byte[] key, final byte[] field) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.hget(key, field);
		} finally {
			j.close();
		}
	}

	@Override
	public Long hsetnx(final byte[] key, final byte[] field, final byte[] value) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.hsetnx(key, field, value);
		} finally {
			j.close();
		}
	}

	@Override
	public String hmset(final byte[] key, final Map<byte[], byte[]> hash) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.hmset(key, hash);
		} finally {
			j.close();
		}
	}

	@Override
	public List<byte[]> hmget(final byte[] key, final byte[]... fields) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.hmget(key, fields);
		} finally {
			j.close();
		}
	}

	@Override
	public Long hincrBy(final byte[] key, final byte[] field, final long value) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.hincrBy(key, field, value);
		} finally {
			j.close();
		}
	}

	@Override
	public Double hincrByFloat(final byte[] key, final byte[] field, final double value) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.hincrByFloat(key, field, value);
		} finally {
			j.close();
		}
	}

	@Override
	public Boolean hexists(final byte[] key, final byte[] field) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.hexists(key, field);
		} finally {
			j.close();
		}
	}

	@Override
	public Long hdel(final byte[] key, final byte[]... fields) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.hdel(key, fields);
		} finally {
			j.close();
		}
	}

	@Override
	public Long hlen(final byte[] key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.hlen(key);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<byte[]> hkeys(final byte[] key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.hkeys(key);
		} finally {
			j.close();
		}
	}

	@Override
	public Collection<byte[]> hvals(final byte[] key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.hvals(key);
		} finally {
			j.close();
		}
	}

	@Override
	public Map<byte[], byte[]> hgetAll(final byte[] key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.hgetAll(key);
		} finally {
			j.close();
		}
	}

	@Override
	public Long rpush(final byte[] key, final byte[]... strings) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.rpush(key, strings);
		} finally {
			j.close();
		}
	}

	@Override
	public Long lpush(final byte[] key, final byte[]... strings) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.lpush(key, strings);
		} finally {
			j.close();
		}
	}

	@Override
	public Long strlen(final byte[] key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.strlen(key);
		} finally {
			j.close();
		}
	}

	@Override
	public Long lpushx(final byte[] key, final byte[]... string) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.lpushx(key, string);
		} finally {
			j.close();
		}
	}

	@Override
	public Long persist(final byte[] key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.persist(key);
		} finally {
			j.close();
		}
	}

	@Override
	public Long rpushx(final byte[] key, final byte[]... string) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.rpushx(key, string);
		} finally {
			j.close();
		}
	}

	@Override
	public Long llen(final byte[] key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.llen(key);
		} finally {
			j.close();
		}
	}

	@Override
	public List<byte[]> lrange(final byte[] key, final long start, final long stop) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.lrange(key, start, stop);
		} finally {
			j.close();
		}
	}

	@Override
	public String ltrim(final byte[] key, final long start, final long stop) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.ltrim(key, start, stop);
		} finally {
			j.close();
		}
	}

	@Override
	public byte[] lindex(final byte[] key, final long index) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.lindex(key, index);
		} finally {
			j.close();
		}
	}

	@Override
	public String lset(final byte[] key, final long index, final byte[] value) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.lset(key, index, value);
		} finally {
			j.close();
		}
	}

	@Override
	public Long lrem(final byte[] key, final long count, final byte[] value) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.lrem(key, count, value);
		} finally {
			j.close();
		}
	}

	@Override
	public byte[] lpop(final byte[] key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.lpop(key);
		} finally {
			j.close();
		}
	}

	@Override
	public byte[] rpop(final byte[] key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.rpop(key);
		} finally {
			j.close();
		}
	}

	@Override
	public Long sadd(final byte[] key, final byte[]... members) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.sadd(key, members);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<byte[]> smembers(final byte[] key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.smembers(key);
		} finally {
			j.close();
		}
	}

	@Override
	public Long srem(final byte[] key, final byte[]... members) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.srem(key, members);
		} finally {
			j.close();
		}
	}

	@Override
	public byte[] spop(final byte[] key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.spop(key);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<byte[]> spop(final byte[] key, final long count) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.spop(key, count);
		} finally {
			j.close();
		}
	}

	@Override
	public Long scard(final byte[] key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.scard(key);
		} finally {
			j.close();
		}
	}

	@Override
	public Boolean sismember(final byte[] key, final byte[] member) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.sismember(key, member);
		} finally {
			j.close();
		}
	}

	@Override
	public byte[] srandmember(final byte[] key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.srandmember(key);
		} finally {
			j.close();
		}
	}

	@Override
	public List srandmember(final byte[] key, final int count) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.srandmember(key, count);
		} finally {
			j.close();
		}
	}

	@Override
	public Long zadd(final byte[] key, final double score, final byte[] member) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zadd(key, score, member);
		} finally {
			j.close();
		}
	}

	@Override
	public Long zadd(final byte[] key, final double score, final byte[] member, final ZAddParams params) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zadd(key, score, member, params);
		} finally {
			j.close();
		}
	}

	@Override
	public Long zadd(final byte[] key, final Map<byte[], Double> scoreMembers) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zadd(key, scoreMembers);
		} finally {
			j.close();
		}
	}

	@Override
	public Long zadd(final byte[] key, final Map<byte[], Double> scoreMembers, final ZAddParams params) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zadd(key, scoreMembers, params);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<byte[]> zrange(final byte[] key, final long start, final long stop) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrange(key, start, stop);
		} finally {
			j.close();
		}
	}

	@Override
	public Long zrem(final byte[] key, final byte[]... members) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrem(key, members);
		} finally {
			j.close();
		}
	}

	@Override
	public Double zincrby(final byte[] key, final double increment, final byte[] member) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zincrby(key, increment, member);
		} finally {
			j.close();
		}
	}

	@Override
	public Double zincrby(final byte[] key, final double increment, final byte[] member, ZIncrByParams params) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zincrby(key, increment, member, params);
		} finally {
			j.close();
		}
	}

	@Override
	public Long zrank(final byte[] key, final byte[] member) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrank(key, member);
		} finally {
			j.close();
		}
	}

	@Override
	public Long zrevrank(final byte[] key, final byte[] member) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrevrank(key, member);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<byte[]> zrevrange(final byte[] key, final long start, final long stop) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrevrange(key, start, stop);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<Tuple> zrangeWithScores(final byte[] key, final long start, final long stop) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrangeWithScores(key, start, stop);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<Tuple> zrevrangeWithScores(final byte[] key, final long start, final long stop) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrevrangeWithScores(key, start, stop);
		} finally {
			j.close();
		}
	}

	@Override
	public Long zcard(final byte[] key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zcard(key);
		} finally {
			j.close();
		}
	}

	@Override
	public Double zscore(final byte[] key, final byte[] member) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zscore(key, member);
		} finally {
			j.close();
		}
	}

	@Override
	public List<byte[]> sort(final byte[] key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.sort(key);
		} finally {
			j.close();
		}
	}

	@Override
	public List<byte[]> sort(final byte[] key, SortingParams sortingParameters) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.sort(key, sortingParameters);
		} finally {
			j.close();
		}
	}

	@Override
	public Long zcount(final byte[] key, final double min, final double max) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zcount(key, min, max);
		} finally {
			j.close();
		}
	}

	@Override
	public Long zcount(final byte[] key, final byte[] min, final byte[] max) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zcount(key, min, max);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<byte[]> zrangeByScore(final byte[] key, final double min, final double max) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrangeByScore(key, min, max);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<byte[]> zrangeByScore(final byte[] key, final double min, final double max, final int offset,
			final int count) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrangeByScore(key, min, max, offset, count);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<Tuple> zrangeByScoreWithScores(final byte[] key, final double min, final double max) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrangeByScoreWithScores(key, min, max);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<Tuple> zrangeByScoreWithScores(final byte[] key, final double min, final double max, final int offset,
			final int count) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrangeByScoreWithScores(key, min, max, offset, count);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<byte[]> zrangeByScore(final byte[] key, final byte[] min, final byte[] max) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrangeByScore(key, min, max);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<Tuple> zrangeByScoreWithScores(final byte[] key, final byte[] min, final byte[] max) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrangeByScoreWithScores(key, min, max);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<Tuple> zrangeByScoreWithScores(final byte[] key, final byte[] min, final byte[] max, final int offset,
			final int count) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrangeByScoreWithScores(key, min, max, offset, count);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<byte[]> zrangeByScore(final byte[] key, final byte[] min, final byte[] max, final int offset,
			final int count) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrangeByScore(key, min, max, offset, count);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<byte[]> zrevrangeByScore(final byte[] key, final double max, final double min) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrevrangeByScore(key, max, min);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<byte[]> zrevrangeByScore(final byte[] key, final double max, final double min, final int offset,
			final int count) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrevrangeByScore(key, max, min, offset, count);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<Tuple> zrevrangeByScoreWithScores(final byte[] key, final double max, final double min) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrevrangeByScoreWithScores(key, max, min);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<Tuple> zrevrangeByScoreWithScores(final byte[] key, final double max, final double min, final int offset,
			final int count) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrevrangeByScoreWithScores(key, max, min, offset, count);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<byte[]> zrevrangeByScore(final byte[] key, final byte[] max, final byte[] min) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrevrangeByScore(key, max, min);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<byte[]> zrevrangeByScore(final byte[] key, final byte[] max, final byte[] min, final int offset,
			final int count) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrevrangeByScore(key, max, min, offset, count);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<Tuple> zrevrangeByScoreWithScores(final byte[] key, final byte[] max, final byte[] min) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrevrangeByScoreWithScores(key, max, min);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<Tuple> zrevrangeByScoreWithScores(final byte[] key, final byte[] max, final byte[] min, final int offset,
			final int count) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrevrangeByScoreWithScores(key, max, min, offset, count);
		} finally {
			j.close();
		}
	}

	@Override
	public Long zremrangeByRank(final byte[] key, final long start, final long stop) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zremrangeByRank(key, start, stop);
		} finally {
			j.close();
		}
	}

	@Override
	public Long zremrangeByScore(final byte[] key, final double min, final double max) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zremrangeByScore(key, min, max);
		} finally {
			j.close();
		}
	}

	@Override
	public Long zremrangeByScore(final byte[] key, final byte[] min, final byte[] max) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zremrangeByScore(key, min, max);
		} finally {
			j.close();
		}
	}

	@Override
	public Long zlexcount(final byte[] key, final byte[] min, final byte[] max) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zlexcount(key, min, max);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<byte[]> zrangeByLex(final byte[] key, final byte[] min, final byte[] max) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrangeByLex(key, min, max);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<byte[]> zrangeByLex(final byte[] key, final byte[] min, final byte[] max, final int offset,
			final int count) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrangeByLex(key, min, max, offset, count);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<byte[]> zrevrangeByLex(final byte[] key, final byte[] max, final byte[] min) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrevrangeByLex(key, max, min);
		} finally {
			j.close();
		}
	}

	@Override
	public Set<byte[]> zrevrangeByLex(final byte[] key, final byte[] max, final byte[] min, final int offset,
			final int count) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zrevrangeByLex(key, max, min, offset, count);
		} finally {
			j.close();
		}
	}

	@Override
	public Long zremrangeByLex(final byte[] key, final byte[] min, final byte[] max) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zremrangeByLex(key, min, max);
		} finally {
			j.close();
		}
	}

	@Override
	public Long linsert(final byte[] key, final ListPosition where, final byte[] pivot, final byte[] value) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.linsert(key, where, pivot, value);
		} finally {
			j.close();
		}
	}

	// public ShardedJedisPipeline pipelined() {
	// ShardedJedisPipeline pipeline = new ShardedJedisPipeline();
	// pipeline.setShardedJedis(this);
	// return pipeline;
	// }

	public Long objectRefcount(final byte[] key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.objectRefcount(key);
		} finally {
			j.close();
		}
	}

	public byte[] objectEncoding(final byte[] key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.objectEncoding(key);
		} finally {
			j.close();
		}
	}

	public Long objectIdletime(final byte[] key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.objectIdletime(key);
		} finally {
			j.close();
		}
	}

	@Override
	public Boolean setbit(final byte[] key, final long offset, boolean value) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.setbit(key, offset, value);
		} finally {
			j.close();
		}
	}

	@Override
	public Boolean setbit(final byte[] key, final long offset, final byte[] value) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.setbit(key, offset, value);
		} finally {
			j.close();
		}
	}

	@Override
	public Boolean getbit(final byte[] key, final long offset) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.getbit(key, offset);
		} finally {
			j.close();
		}
	}

	@Override
	public Long setrange(final byte[] key, final long offset, final byte[] value) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.setrange(key, offset, value);
		} finally {
			j.close();
		}
	}

	@Override
	public byte[] getrange(final byte[] key, final long startOffset, final long endOffset) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.getrange(key, startOffset, endOffset);
		} finally {
			j.close();
		}
	}

	@Override
	public Long move(final byte[] key, final int dbIndex) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.move(key, dbIndex);
		} finally {
			j.close();
		}
	}

	@Override
	public byte[] echo(final byte[] arg) {
		JedisSentinelPool p = getShard(arg);
		Jedis j = p.getResource();
		try {
			return j.echo(arg);
		} finally {
			j.close();
		}
	}

	public List<byte[]> brpop(final byte[] arg) {
		JedisSentinelPool p = getShard(arg);
		Jedis j = p.getResource();
		try {
			return j.brpop(arg);
		} finally {
			j.close();
		}
	}

	public List<byte[]> blpop(final byte[] arg) {
		JedisSentinelPool p = getShard(arg);
		Jedis j = p.getResource();
		try {
			return j.blpop(arg);
		} finally {
			j.close();
		}
	}

	@Override
	public Long bitcount(final byte[] key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.bitcount(key);
		} finally {
			j.close();
		}
	}

	@Override
	public Long bitcount(final byte[] key, final long start, final long end) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.bitcount(key, start, end);
		} finally {
			j.close();
		}
	}

	@Override
	public Long pfadd(final byte[] key, final byte[]... elements) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.pfadd(key, elements);
		} finally {
			j.close();
		}
	}

	@Override
	public long pfcount(final byte[] key) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.pfcount(key);
		} finally {
			j.close();
		}
	}

	@Override
	public Long geoadd(final byte[] key, final double longitude, final double latitude, final byte[] member) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.geoadd(key, longitude, latitude, member);
		} finally {
			j.close();
		}
	}

	@Override
	public Long geoadd(final byte[] key, final Map<byte[], GeoCoordinate> memberCoordinateMap) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.geoadd(key, memberCoordinateMap);
		} finally {
			j.close();
		}
	}

	@Override
	public Double geodist(final byte[] key, final byte[] member1, final byte[] member2) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.geodist(key, member1, member2);
		} finally {
			j.close();
		}
	}

	@Override
	public Double geodist(final byte[] key, final byte[] member1, final byte[] member2, final GeoUnit unit) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.geodist(key, member1, member2, unit);
		} finally {
			j.close();
		}
	}

	@Override
	public List<byte[]> geohash(final byte[] key, final byte[]... members) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.geohash(key, members);
		} finally {
			j.close();
		}
	}

	@Override
	public List<GeoCoordinate> geopos(final byte[] key, final byte[]... members) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.geopos(key, members);
		} finally {
			j.close();
		}
	}

	@Override
	public List<GeoRadiusResponse> georadius(final byte[] key, final double longitude, final double latitude,
			final double radius, final GeoUnit unit) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.georadius(key, longitude, latitude, radius, unit);
		} finally {
			j.close();
		}
	}

	@Override
	public List<GeoRadiusResponse> georadius(final byte[] key, final double longitude, final double latitude,
			final double radius, final GeoUnit unit, final GeoRadiusParam param) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.georadius(key, longitude, latitude, radius, unit, param);
		} finally {
			j.close();
		}
	}

	@Override
	public List<GeoRadiusResponse> georadiusByMember(final byte[] key, final byte[] member, final double radius,
			final GeoUnit unit) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.georadiusByMember(key, member, radius, unit);
		} finally {
			j.close();
		}
	}

	@Override
	public List<GeoRadiusResponse> georadiusByMember(final byte[] key, final byte[] member, final double radius,
			final GeoUnit unit, final GeoRadiusParam param) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.georadiusByMember(key, member, radius, unit, param);
		} finally {
			j.close();
		}
	}

	@Override
	public ScanResult<Map.Entry<byte[], byte[]>> hscan(final byte[] key, final byte[] cursor) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.hscan(key, cursor);
		} finally {
			j.close();
		}
	}

	@Override
	public ScanResult<Map.Entry<byte[], byte[]>> hscan(final byte[] key, final byte[] cursor, final ScanParams params) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.hscan(key, cursor, params);
		} finally {
			j.close();
		}
	}

	@Override
	public ScanResult<byte[]> sscan(final byte[] key, final byte[] cursor) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.sscan(key, cursor);
		} finally {
			j.close();
		}
	}

	@Override
	public ScanResult<byte[]> sscan(final byte[] key, final byte[] cursor, final ScanParams params) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.sscan(key, cursor, params);
		} finally {
			j.close();
		}
	}

	@Override
	public ScanResult<Tuple> zscan(final byte[] key, final byte[] cursor) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zscan(key, cursor);
		} finally {
			j.close();
		}
	}

	@Override
	public ScanResult<Tuple> zscan(final byte[] key, final byte[] cursor, final ScanParams params) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.zscan(key, cursor, params);
		} finally {
			j.close();
		}
	}

	@Override
	public List<Long> bitfield(final byte[] key, final byte[]... arguments) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.bitfield(key, arguments);
		} finally {
			j.close();
		}
	}

	@Override
	public Long hstrlen(final byte[] key, final byte[] field) {
		JedisSentinelPool p = getShard(key);
		Jedis j = p.getResource();
		try {
			return j.hstrlen(key, field);
		} finally {
			j.close();
		}
	}
}
