/**
 * @Description:
 * @date:   2018年12月4日 上午9:25:36   
 * @version V1.0
 */
package redis.clients.jedis;

import redis.clients.jedis.util.ShardInfo;
import redis.clients.jedis.util.Sharded;
import redis.clients.jedis.util.Pool;

/**
 * @Description: 
 *   
 * @author: 李光强
 * @date:   2018年12月4日 上午9:25:36
 */
public class SentinelShareInfo extends ShardInfo<JedisSentinelPool> {

	private String name = null;
	private JedisSentinelPool pool;
	
	public SentinelShareInfo(JedisSentinelPool pool) {
		this(null, pool);
	}
	
	public SentinelShareInfo(String name, JedisSentinelPool pool) {
		this(null, pool, Sharded.DEFAULT_WEIGHT);
	}
	
	public SentinelShareInfo(String name, JedisSentinelPool pool, int weight) {
		super(weight);
		this.name = name;
		this.pool = pool;
	}
	
	@Override
	protected JedisSentinelPool createResource() {
		return pool;
	}

	@Override
	public String getName() {
		return name;
	}

}
