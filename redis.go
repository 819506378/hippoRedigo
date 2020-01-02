package hippoRedigo

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"io/ioutil"
	"os"
	"strconv"
	"time"
)

type RedigoConfig struct {
	Ip                   string        `json:"ip"`                      // redis 服务器ip地址
	Port                 int           `json:"port"`                    // redis 服务器端口
	Username             string        `json:"username"`                // redis 登录帐号
	Password             string        `json:"password"`                // redis 登录密码
	DatabaseIndex        int           `json:"database_index"`          // redis 数据库编号
	MaxConnectionNum     int           `json:"max_connection_num"`      // redis 最大连接数
	MaxIdleConnectionNum int           `json:"max_idle_connection_num"` // redis 最大空闲连接数
	IdleTimeout          time.Duration `json:"idle_timeout"`            // redis 空闲连接超时时间
}

const (
	hippoRedigoDefaultConfigDir      = "./hippoConfig"
	hippoRedigoDefaultConfigFileName = "hippoRedigoConfig.json"
	hippoRedigoDefaultConfigJson     = `{
	"redis服务器ip地址":"ip（默认ip 127.0.0.1）",
	"ip":"127.0.0.1",
	"redis服务器端口":"port （默认端口 6379）",
	"port":6379,
	"redis用户名":"username（默认用户名 AUTH）",
	"username":"AUTH",
	"redis登录密码":"password （默认密码 空）",
	"password":"",
	"redis数据库序号":"database_index（默认序号 0）",
	"database_index":0,
	"redis最大连接数":"max_connection_num（默认最大连接数 50）",
	"max_connection_num":50,
	"redis最大空闲连接数":"max_idle_connection_num（默认最大空闲连接数 10）",
	"max_idle_connection_num":10,
	"redis空闲连接超时时间":"idle_timeout（单位秒，默认超时时间 300）",
	"idle_timeout":300
}
	`
)

var (
	pool                 *redis.Pool
	ErrNil               = redis.ErrNil
	ErrTTLNotSet         = errors.New("ttl is not set")
	ErrKeyNotExist       = errors.New("key does not exist")
	ErrDestinationNotSet = errors.New("destination is not set")
	ErrKeysNotSet        = errors.New("keys are not set")
)

func Init(configFilePath string) {
	if pool != nil {
		panic(errors.New(fmt.Sprintf("redigo has inited")))
	}

	hippoRedigoConfig, err := readConfig(configFilePath)
	if nil != err {
		panic(err)
	}

	var e error
	pool = &redis.Pool{
		MaxIdle:     hippoRedigoConfig.MaxIdleConnectionNum,
		MaxActive:   hippoRedigoConfig.MaxConnectionNum,
		Wait:        true,
		IdleTimeout: hippoRedigoConfig.IdleTimeout * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp",
				fmt.Sprintf("%s:%d", hippoRedigoConfig.Ip, hippoRedigoConfig.Port),
				redis.DialDatabase(hippoRedigoConfig.DatabaseIndex),
				redis.DialPassword(hippoRedigoConfig.Password))
			if err != nil {
				e = err
				return nil, err
			}
			return c, nil
		},
	}

	if e != nil {
		panic(fmt.Sprintf("create redis pool has error: %v", e))
	}
}

func readConfig(configFilePath string) (*RedigoConfig, error) {
	var (
		configBytes []byte
		err         error
	)
	bConfig := false

	/* 如果用户没有设置或者设置错误的配置文件路径，则自动生成默认配置 */
	if "" != configFilePath {
		if _, err = os.Stat(configFilePath); nil == err {
			bConfig = true
		} else {
			fmt.Println("程序设置的redis配置文件不存在，生成默认配置文件")
		}
	}

	if bConfig {
		configBytes, err = ioutil.ReadFile(configFilePath)
	} else { // 自动生成默认配置文件环境
		configBytes, err = createDefaultHippoRedigoConfigEnvironment()
	}

	if nil != err {
		return nil, err
	}

	redigoConfig := &RedigoConfig{}
	if err := json.Unmarshal(configBytes, redigoConfig); nil != err {
		return nil, err
	}

	return redigoConfig, nil
}

func createDefaultHippoRedigoConfigEnvironment() ([]byte, error) {
	_, err := os.Stat(hippoRedigoDefaultConfigDir)
	if os.IsNotExist(err) {
		if err := os.Mkdir(hippoRedigoDefaultConfigDir, os.ModePerm); nil != err {
			fmt.Println("Create Dir Failed:", err.Error())
			return nil, err
		}
	}

	_, err = os.Stat(hippoRedigoDefaultConfigDir + "/" + hippoRedigoDefaultConfigFileName)
	if !os.IsNotExist(err) {
		return ioutil.ReadFile(hippoRedigoDefaultConfigDir + "/" + hippoRedigoDefaultConfigFileName)
	}

	hippoRedigoConfigJson, err := os.Create(hippoRedigoDefaultConfigDir + "/" + hippoRedigoDefaultConfigFileName)
	if nil != err {
		fmt.Println("Create File Failed:", err.Error())
		return nil, err
	}
	defer hippoRedigoConfigJson.Close()

	if _, err = hippoRedigoConfigJson.WriteString(hippoRedigoDefaultConfigJson); nil != err {
		return nil, err
	}

	return []byte(hippoRedigoDefaultConfigJson), nil
}

type RedisSession struct {
	pool   *redis.Pool
	prefix string
}

func NewSessionWithPrefix(prefix string) *RedisSession {
	rdsSess := newRedisSessionWithPool(pool)
	rdsSess.SetPrefix(prefix)
	return rdsSess
}

func newRedisSessionWithPool(pool *redis.Pool) *RedisSession {
	s := &RedisSession{}
	s.pool = pool

	return s
}

// Pool Returns the connection pool for redis
func (r *RedisSession) Pool() *redis.Pool {
	return r.pool
}

// Close closes the connection pool for redis
func (r *RedisSession) Close() error {
	return r.pool.Close()
}

////////////////////////////////////////////////////////////////////////////////////////
/*Redis Prefix Begin*/
////////////////////////////////////////////////////////////////////////////////////////

// SetPrefix is used to add a prefix to all keys to be used. It is useful for
// creating namespaces for each different application
func (r *RedisSession) SetPrefix(name string) {
	r.prefix = name + ":"
}

func (r *RedisSession) AddPrefix(name string) string {
	return r.prefix + name
}

func (r *RedisSession) GetPrefix() string {
	return r.prefix
}

////////////////////////////////////////////////////////////////////////////////////////
/*Redis Prefix End*/
////////////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////////////
/*Redis Command Begin*/
////////////////////////////////////////////////////////////////////////////////////////

// Do is a wrapper around redigo's redis.Do method that executes any redis
// command. Do does not support prefix support. Example usage: hippoRedigo.Do("INCR",
// "counter").
func (r *RedisSession) Do(cmd string, args ...interface{}) (interface{}, error) {
	conn := r.pool.Get()
	// conn.Close() returns an error but we are already returning regarding error
	// while returning the Do(..) response
	defer conn.Close()
	return conn.Do(cmd, args...)
}

// KeyTTL returns remaining TTL value of the given key. An error is returned
// when TTL is not existed or key is not found
func (r *RedisSession) KeyTTL(key string) (time.Duration, error) {
	reply, err := redis.Int(r.Do("TTL", r.AddPrefix(key)))
	if err != nil {
		return 0, err
	}

	if reply == -1 {
		return 0, ErrTTLNotSet
	}

	if reply == -2 {
		return 0, ErrKeyNotExist
	}

	return time.Duration(reply) * time.Second, nil
}

// KeyPTTL returns remaining PTTL value of the given key. An error is returned
// when PTTL is not existed or key is not found
func (r *RedisSession) KeyPTTL(key string) (time.Duration, error) {
	reply, err := redis.Int(r.Do("PTTL", r.AddPrefix(key)))
	if err != nil {
		return 0, err
	}

	if reply == -1 {
		return 0, ErrTTLNotSet
	}

	if reply == -2 {
		return 0, ErrKeyNotExist
	}

	return time.Duration(reply) * time.Second, nil
}

func (r *RedisSession) KeyExpire(key string, expireSecond int) (int, error) {
	return redis.Int(r.Do("EXPIRE", r.AddPrefix(key), expireSecond))
}

// KeyExpireat expireTimestamp(s)
func (r *RedisSession) KeyExpireat(key string, expireTimestamp int64) (int, error) {
	return redis.Int(r.Do("EXPIREAT", r.AddPrefix(key), expireTimestamp))
}

func (r *RedisSession) KeyPExpire(key string, expireMilliSecond int) (int, error) {
	return redis.Int(r.Do("PEXPIRE", r.AddPrefix(key), expireMilliSecond))
}

// KeyPExpireat expireTimestamp(ms)
func (r *RedisSession) KeyPExpireat(key string, expireTimestamp int64) (int, error) {
	return redis.Int(r.Do("PEXPIREAT", r.AddPrefix(key), expireTimestamp))
}

func (r *RedisSession) KeyRename(oldKey, newKey string) error {
	reply, err := redis.String(r.Do("RENAME", r.AddPrefix(oldKey), r.AddPrefix(newKey)))
	if err != nil {
		return err
	}

	if reply != "OK" {
		return fmt.Errorf("reply string is wrong!: %s", reply)
	}

	return nil
}

func (r *RedisSession) KeyRenamenx(oldKey, newKey string) (int, error) {
	return redis.Int(redis.String(r.Do("RENAMENX", r.AddPrefix(oldKey), r.AddPrefix(newKey))))
}

// KeyDelete is used to remove the specified keys. Key is ignored if it does not
// exist. It returns the number of keys that were removed. Example usage:
// hippoRedigo.KeyDelete("counter", "arslan:name")
func (r *RedisSession) KeyDelete(args ...interface{}) (int, error) {
	prefixed := make([]interface{}, 0)
	for _, arg := range args {
		prefixed = append(prefixed, r.AddPrefix(arg.(string)))
	}

	return redis.Int(r.Do("DEL", prefixed...))
}

// KeyExists returns true if key exists or false if not.
func (r *RedisSession) KeyExists(key string) bool {
	// does not have any err message to be checked, it return either 1 or 0
	reply, _ := redis.Int(r.Do("EXISTS", r.AddPrefix(key)))

	return reply == 1 // false means key does not exist
}

// KeyKeys returns all keys with given pattern
// WARNING: Redis Doc says: "Don't use KEYS in your regular application code."
func (r *RedisSession) KeyKeys(key string) ([]interface{}, error) {
	return redis.Values(r.Do("KEYS", r.AddPrefix(key)))
}

// StringSet is used to hold the string value. If key already holds a value, it is
// overwritten, regardless of its type. A return of nil means successfull.
// Example usage: hippoRedigo.StringSet("arslan:name", "fatih")
func (r *RedisSession) StringSet(key, value string) error {
	reply, err := r.Do("SET", r.AddPrefix(key), value)
	if err != nil {
		return err
	}

	if reply != "OK" {
		return fmt.Errorf("reply string is wrong!: %s", reply)

	}
	return nil
}

// StringSetex key to hold the string value and set key to timeout after a given
// number of seconds. This command is equivalent to executing the following commands:
// SET mykey value
// EXPIRE mykey seconds
// SETEX is atomic, and can be reproduced by using the previous two
// commands inside an MULTI / EXEC block. It is provided as a faster alternative
// to the given sequence of operations, because this operation is very common
// when Redis is used as a cache.
// An error is returned when seconds is invalid.
func (r *RedisSession) StringSetex(key string, timeout time.Duration, item interface{}) error {
	reply, err := redis.String(r.Do("SETEX", r.AddPrefix(key), strconv.Itoa(int(timeout.Seconds())), item))
	if err != nil {
		return err
	}

	if reply != "OK" {
		return fmt.Errorf("reply string is wrong!: %s", reply)
	}

	return nil
}

func (r *RedisSession) StringPSetex(key string, timeoutMilliSecond int64, item interface{}) error {
	reply, err := redis.String(r.Do("PSETEX", r.AddPrefix(key), strconv.FormatInt(timeoutMilliSecond, 10), item))
	if err != nil {
		return err
	}

	if reply != "OK" {
		return fmt.Errorf("reply string is wrong!: %s", reply)
	}

	return nil
}

func (r *RedisSession) StringSetnx(key, str string) (int, error) {
	return redis.Int(r.Do("SETNX", r.AddPrefix(key), str))
}

func (r *RedisSession) StringStrlen(key string) (int, error) {
	return redis.Int(r.Do("STRLEN", r.AddPrefix(key)))
}

// Get is used to get the value of key. If the key does not exist an empty
// string is returned. Usage: hippoRedigo.StringGet("arslan")
func (r *RedisSession) StringGet(key string) (string, error) {
	reply, err := redis.String(r.Do("GET", r.AddPrefix(key)))
	if err != nil {
		return "", err
	}
	return reply, nil
}

func (r *RedisSession) StringGetSet(key, str string) (string, error) {
	return redis.String(r.Do("GETSET", r.AddPrefix(key), str))
}

// prepareArgsWithKey helper method prepends key to given variadic parameter
func (r *RedisSession) prepareArgsWithKey(key string, rest ...interface{}) []interface{} {
	prefixedReq := make([]interface{}, len(rest)+1)

	// prepend prefixed key
	prefixedReq[0] = r.AddPrefix(key)

	for key, el := range rest {
		prefixedReq[key+1] = el
	}

	return prefixedReq
}

func (r *RedisSession) prepareKeys(keys ...string) []string {
	var keysWithPrefixKey []string
	for _, key := range keys {
		keysWithPrefixKey = append(keysWithPrefixKey, key)
	}
	return keysWithPrefixKey
}

// SetSadd adds given elements to the set stored at key. Given elements
// that are already included in set are ignored.
// Returns successfully added key count and error state
func (r *RedisSession) SetSadd(key string, rest ...interface{}) (int, error) {
	return redis.Int(r.Do("SADD", r.prepareArgsWithKey(key, rest...)...))
}

// SetScard gets the member count of a Set with given key
func (r *RedisSession) SetScard(key string) (int, error) {
	return redis.Int(r.Do("SCARD", r.AddPrefix(key)))
}

func (r *RedisSession) SetSdiff(keys ...string) ([]interface{}, error) {
	return redis.Values(r.Do("SDIFF", r.prepareKeys(keys...)))
}

func (r *RedisSession) SetSdiffstore(newKey string, keys ...string) ([]interface{}, error) {
	return redis.Values(r.Do("SDIFFSTORE", r.AddPrefix(newKey), r.prepareKeys(keys...)))
}

func (r *RedisSession) SetSinter(keys ...string) ([]interface{}, error) {
	return redis.Values(r.Do("SINTER", r.prepareKeys(keys...)))
}

func (r *RedisSession) SetSinterstore(newKey string, keys ...string) ([]interface{}, error) {
	return redis.Values(r.Do("SINTERSTORE", r.AddPrefix(newKey), r.prepareKeys(keys...)))
}

func (r *RedisSession) SetSismember(key, member string) (int, error) {
	return redis.Int(r.Do("SISMEMBER", r.prepareArgsWithKey(key, member)...))
}

func (r *RedisSession) SetSmembers(key string) ([]interface{}, error) {
	return redis.Values(r.Do("SMEMBERS", r.AddPrefix(key)))
}

func (r *RedisSession) SetSpopCount(key string, count int) ([]interface{}, error) {
	return redis.Values(r.Do("SPOP", r.AddPrefix(key), strconv.Itoa(count)))
}

func (r *RedisSession) SetSpop(key string) (string, error) {
	return redis.String(r.Do("SPOP", r.AddPrefix(key)))
}

func (r *RedisSession) SetSrandmemberCount(key string, count int) ([]interface{}, error) {
	return redis.Values(r.Do("SRANDMEMBER", r.AddPrefix(key), strconv.Itoa(count)))
}

func (r *RedisSession) SetSrandmember(key string) (string, error) {
	return redis.String(r.Do("SRANDMEMBER", r.AddPrefix(key)))
}

func (r *RedisSession) SetSrem(key string, rest ...interface{}) (int, error) {
	return redis.Int(r.Do("SREM", r.prepareArgsWithKey(key, rest...)))
}

func (r *RedisSession) SetSunion(keys ...string) ([]interface{}, error) {
	return redis.Values(r.Do("SUNION", r.prepareKeys(keys...)))
}

func (r *RedisSession) SetSunionstore(newKey string, keys ...string) (int, error) {
	return redis.Int(r.Do("SUNIONSTORE", r.AddPrefix(newKey), r.prepareKeys(keys...)))
}

func (r *RedisSession) HashHset(key, field, value string) (int, error) {
	return redis.Int(r.Do("HSET", r.AddPrefix(key), field, value))
}

func (r *RedisSession) HashHsetnx(key, field, value string) (int, error) {
	return redis.Int(r.Do("HSETNX", r.AddPrefix(key), field, value))
}

func (r *RedisSession) HashHget(key string, fields ...string) (interface{}, error) {
	return redis.String(r.Do("HGET", r.prepareArgsWithKey(key, fields)))
}

func (r *RedisSession) HashHgetall(key string) ([]interface{}, error) {
	return redis.Values(r.Do("HGETALL", r.AddPrefix(key)))
}

func (r *RedisSession) HashHexist(key, field string) (int, error) {
	return redis.Int(r.Do("HEXIST", r.AddPrefix(key), field))
}

func (r *RedisSession) HashHdel(key string, fields ...string) (int, error) {
	return redis.Int(r.Do("HDEL", r.prepareArgsWithKey(key, fields)))
}

func (r *RedisSession) HashHkeys(key string) ([]interface{}, error) {
	return redis.Values(r.Do("HKEYS", r.AddPrefix(key)))
}

func (r *RedisSession) HashHlen(key string) (int, error) {
	return redis.Int(r.Do("HLEN", r.AddPrefix(key)))
}

func (r *RedisSession) HashHmget(key string, fields ...string) ([]interface{}, error) {
	return redis.Values(r.Do("HMGET", r.prepareArgsWithKey(key, fields)))
}

func (r *RedisSession) ListLpush(key string, values ...interface{}) (int, error) {
	return redis.Int(r.Do("LPUSH", r.prepareArgsWithKey(key, values)))
}

func (r *RedisSession) ListLpushx(key, value string) (int, error) {
	return redis.Int(r.Do("LPUSHX", r.AddPrefix(key), value))
}

func (r *RedisSession) ListRpush(key string, values ...interface{}) (int, error) {
	return redis.Int(r.Do("RPUSH", r.prepareArgsWithKey(key, values)))
}

func (r *RedisSession) ListRpushx(key, value string) (int, error) {
	return redis.Int(r.Do("RPUSHX", r.AddPrefix(key), value))
}

func (r *RedisSession) ListLpop(key string) (string, error) {
	return redis.String(r.Do("LPOP", r.AddPrefix(key)))
}

func (r *RedisSession) ListRpop(key string) (string, error) {
	return redis.String(r.Do("RPOP", r.AddPrefix(key)))
}

func (r *RedisSession) ListLrange(key string, start, end int) ([]interface{}, error) {
	return redis.Values(r.Do("LRANGE", r.AddPrefix(key), strconv.Itoa(start), strconv.Itoa(end)))
}

func (r *RedisSession) ListLlen(key string) (int, error) {
	return redis.Int(r.Do("LLEN", r.AddPrefix(key)))
}

func (r *RedisSession) ListLinsert(key, pivot, value string, bBefore bool) (int, error) {
	if bBefore {
		return redis.Int(r.Do("LINSERT", r.AddPrefix(key), "BEFORE", pivot, value))
	} else {
		return redis.Int(r.Do("LINSERT", r.AddPrefix(key), "AFTER", pivot, value))
	}
}

func (r *RedisSession) SortedSetZadd(key, value string, score interface{}) (int, error) {
	return redis.Int(r.Do("ZADD", r.AddPrefix(key), score, value))
}

func (r *RedisSession) SortedSetZcard(key string) (int, error) {
	return redis.Int(r.Do("ZCARD", r.AddPrefix(key)))
}

func (r *RedisSession) SortedSetZcount(key string, min, max interface{}) (int, error) {
	return redis.Int(r.Do("ZCOUNT", r.AddPrefix(key), min, max))
}

// SortedSetsUnion creates a combined set from given list of sorted set keys.
//
// See: http://redis.io/commands/zunionstore
func (r *RedisSession) SortedSetsUnion(destination string, keys []string, weights []interface{}, aggregate string) (int64, error) {
	if destination == "" {
		return 0, ErrDestinationNotSet
	}

	lengthOfKeys := len(keys)
	if lengthOfKeys == 0 {
		return 0, ErrKeysNotSet
	}

	prefixed := []interface{}{
		r.AddPrefix(destination), lengthOfKeys,
	}

	for _, key := range keys {
		prefixed = append(prefixed, r.AddPrefix(key))
	}

	if len(weights) != 0 {
		prefixed = append(prefixed, "WEIGHTS")
		prefixed = append(prefixed, weights...)
	}

	if aggregate != "" {
		prefixed = append(prefixed, "AGGREGATE", aggregate)
	}

	return redis.Int64(r.Do("ZUNIONSTORE", prefixed...))
}

// SortedSetScore returns score of a member in a sorted set. If no member,
// an error is returned.
//
// See: http://redis.io/commands/zscore
func (r *RedisSession) SortedSetScore(key string, member interface{}) (float64, error) {
	return redis.Float64(r.Do("ZSCORE", r.AddPrefix(key), member))
}

func (r *RedisSession) SortedSetXincrby(key, member string, incrememnt interface{}) (string, error) {
	return redis.String(r.Do("ZINCRBY", r.AddPrefix(key), incrememnt, member))
}

// SortedSetRem removes a member from a sorted set. If no member, an error
// is returned.
//
// See: http://redis.io/commands/zrem
func (r *RedisSession) SortedSetRem(key string, members ...interface{}) (int64, error) {
	prefixed := []interface{}{r.AddPrefix(key)}
	prefixed = append(prefixed, members...)

	return redis.Int64(r.Do("ZREM", prefixed...))
}

// SortedSetRangebyScore key min max
// returns all the elements in the sorted set at key with a score
// between min and max.
//
// See: http://redis.io/commands/zrangebyscore
func (r *RedisSession) SortedSetRangebyScore(key string, rest ...interface{}) ([]interface{}, error) {
	prefixed := []interface{}{r.AddPrefix(key)}
	prefixed = append(prefixed, rest...)

	return redis.Values(r.Do("ZRANGEBYSCORE", prefixed...))
}

////////////////////////////////////////////////////////////////////////////////////////
/*Redis Command End*/
////////////////////////////////////////////////////////////////////////////////////////

// Bool converts the given value to boolean
func (r *RedisSession) Bool(reply interface{}) (bool, error) {
	return redis.Bool(reply, nil)
}

// Int converts the given value to integer
func (r *RedisSession) Int(reply interface{}) (int, error) {
	return redis.Int(reply, nil)
}

// String converts the given value to string
func (r *RedisSession) String(reply interface{}) (string, error) {
	return redis.String(reply, nil)
}

// Int64 converts the given value to 64 bit integer
func (r *RedisSession) Int64(reply interface{}) (int64, error) {
	return redis.Int64(reply, nil)
}

// Values is a helper that converts an array command reply to a
// []interface{}. If err is not equal to nil, then Values returns nil, err.
// Otherwise, Values converts the reply as follows:
// Reply type      Result
// array           reply, nil
// nil             nil, ErrNil
// other           nil, error
func (r *RedisSession) Values(reply interface{}) ([]interface{}, error) {
	return redis.Values(reply, nil)
}
