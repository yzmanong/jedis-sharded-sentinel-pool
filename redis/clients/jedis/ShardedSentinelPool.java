/**
 * @Description:
 * @date:   2018年11月27日 下午7:24:39   
 * @version V1.0
 */
package redis.clients.jedis;

import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.util.Hashing;
import redis.clients.jedis.util.Pool;

/**
 * @Description:
 * 
 * @author: 李光强
 * @date: 2018年11月27日 下午7:24:39
 */
public class ShardedSentinelPool extends Pool<ShardedSentinel> {

	public ShardedSentinelPool(final GenericObjectPoolConfig poolConfig, List<SentinelShareInfo> shards) {
		this(poolConfig, shards, Hashing.MURMUR_HASH);
	}

	public ShardedSentinelPool(final GenericObjectPoolConfig poolConfig, List<SentinelShareInfo> shards, Hashing algo) {
		this(poolConfig, shards, algo, null);
	}

	public ShardedSentinelPool(final GenericObjectPoolConfig poolConfig, List<SentinelShareInfo> shards,
			Pattern keyTagPattern) {
		this(poolConfig, shards, Hashing.MURMUR_HASH, keyTagPattern);
	}

	public ShardedSentinelPool(final GenericObjectPoolConfig poolConfig, List<SentinelShareInfo> shards, Hashing algo,
			Pattern keyTagPattern) {
		super(poolConfig, new ShardedSentinelFactory(shards, algo, keyTagPattern));
	}

	@Override
	public ShardedSentinel getResource() {
		ShardedSentinel jedis = super.getResource();
		jedis.setDataSource(this);
		return jedis;
	}

	@Override
	protected void returnBrokenResource(final ShardedSentinel resource) {
		if (resource != null) {
			returnBrokenResourceObject(resource);
		}
	}

	@Override
	protected void returnResource(final ShardedSentinel resource) {
		if (resource != null) {
			returnResourceObject(resource);
		}
	}
	
	/**
	 * PoolableObjectFactory custom impl.
	 */
	private static class ShardedSentinelFactory implements PooledObjectFactory<ShardedSentinel> {
		private List<SentinelShareInfo> shards;
		private Hashing algo;
		private Pattern keyTagPattern;

		public ShardedSentinelFactory(List<SentinelShareInfo> shards, Hashing algo, Pattern keyTagPattern) {
			this.shards = shards;
			this.algo = algo;
			this.keyTagPattern = keyTagPattern;
		}

		@Override
		public PooledObject<ShardedSentinel> makeObject() throws Exception {
			ShardedSentinel jedis = new ShardedSentinel(shards, algo, keyTagPattern);
			return new DefaultPooledObject<ShardedSentinel>(jedis);
		}

		@Override
		public void destroyObject(PooledObject<ShardedSentinel> pooledShardedJedis) throws Exception {
			final ShardedSentinel shardedJedis = pooledShardedJedis.getObject();
			for (JedisSentinelPool pool : shardedJedis.getAllShards()) {
				if( !pool.isClosed() )
					pool.close();
			}
		}

		@Override
		public boolean validateObject(PooledObject<ShardedSentinel> pooledShardedJedis) {
			try {
				ShardedSentinel shared = pooledShardedJedis.getObject();
				for (JedisSentinelPool pool : shared.getAllShards()) {
					Jedis jedis = pool.getResource();
					if (!jedis.ping().equals("PONG")) {
						return false;
					}
				}
				return true;
			} catch (Exception ex) {
				return false;
			}
		}

		@Override
		public void activateObject(PooledObject<ShardedSentinel> p) throws Exception {

		}

		@Override
		public void passivateObject(PooledObject<ShardedSentinel> p) throws Exception {

		}
	}
}
