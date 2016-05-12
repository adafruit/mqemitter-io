'use strict'

const Redis = require('ioredis'),
      MQEmitter = require('mqemitter'),
      shortid = require('shortid'),
      LRU = require('lru-cache'),
      msgpack = require('msgpack-lite');

class MQEmitterIO extends MQEmitter {

  constructor(opts) {

    super(opts);

    this.subConn = new Redis(opts);
    this.pubConn = new Redis(opts);

    this._topics = {};

    this._cache = LRU({
      max: 10000,
      maxAge: 60 * 1000 // one minute
    });

    this.subConn.on('messageBuffer', this.handler.bind(this));
    this.subConn.on('pmessageBuffer', this.handler.bind(this));

  }

  handler(sub, topic, payload) {

    const packet = msgpack.decode(payload);

    if(! this._cache.get(packet.id))
      super.emit(packet.msg)

    this._cache.set(packet.id, true);

  }

  close(cb) {

    let count = 2

    const onEnd = () => {
      if(--count === 0) super.close(cb);
    };

    this.subConn.on('end', onEnd)
    this.subConn.quit()

    this.pubConn.on('end', onEnd)
    this.pubConn.quit()

    return this;

  }

  _subTopic(topic) {
    return topic.replace(this._opts.wildcardOne, '*')
                .replace(this._opts.wildcardSome, '*')
  }

  on(topic, cb, done) {

    const subTopic = this._subTopic(topic);

    const onFinish = () => {
      if(done) setImmediate(done)
    };

    super.on(topic, cb);

    if(this._topics[subTopic]) {
      this._topics[subTopic]++;
      onFinish();
      return this;
    }

    this._topics[subTopic] = 1;

    if(this._containsWildcard(topic))
      this.subConn.psubscribe(subTopic, onFinish);
    else
      this.subConn.subscribe(subTopic, onFinish);

    return this;

  }

  emit(msg, done) {

    if(this.closed)
      return done(new Error('mqemitter-io is closed'))

    setImmediate(done);

  }

  removeListener(topic, cb, done) {

    const subTopic = this._subTopic(topic);

    const onFinish = () => {
      if(done) setImmediate(done);
    };

    this._removeListener(topic, cb)

    if(--this._topics[subTopic] > 0) {
      onFinish();
      return this;
    }

    delete this._topics[subTopic];

    if (this._containsWildcard(topic))
      this.subConn.punsubscribe(subTopic, onFinish);
    else if (this._matcher.match(topic))
      this.subConn.unsubscribe(subTopic, onFinish);

    return this;

  }

  _containsWildcard(topic) {
    return (topic.indexOf(this._opts.wildcardOne) >= 0) || (topic.indexOf(this._opts.wildcardSome) >= 0);
  }

}

exports = module.exports = MQEmitterIO;
