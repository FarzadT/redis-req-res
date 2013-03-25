var redis = require('redis');
var guid  = require('node-guid');

function RedisReqRes (in_redisConfig) {
  this._pubClient = redis.createClient(in_redisConfig.port, in_redisConfig.host, in_redisConfig.options);
  this._subClient = redis.createClient(in_redisConfig.port, in_redisConfig.host, in_redisConfig.options);

  this._pipeGuid = 'e29a3a25-1fd9-4ade-a2a9-4c3829a87cd5';

  this._callbacks = {};

  var that = this;
  this._subClient.on("message",function(channel, message){
    that._handleRequest(channel, message)
  });
};

RedisReqRes.prototype = {
  on: function(in_channel, in_callback){
    var channel = this._pipeGuid+':'+in_channel;

    this._subClient.subscribe(channel);

    this._callbacks[channel] = in_callback;
  },

  off: function(in_channel){
    var channel = this._pipeGuid+':'+in_channel;
    
    delete this._callbacks[channel];
    
    this._subClient.unsubscribe(channel);
  },

  request: function(in_channel, in_data, in_callback){
    in_data = in_data || {};

    var responseChannel = 'RESPONSE:' + guid.new();

    var data = {query: in_data, responseChannel: responseChannel};

    var that = this;
    this.on(responseChannel, function(data){
      that.off(responseChannel);
      in_callback(data);
    });

    var requestChannel = this._pipeGuid + ':' + in_channel;
    this._pubClient.publish(requestChannel, JSON.stringify(data));
  },

  _response: function(in_channel){
    var that = this;
    function response(in_responseChannel){
      this._responseChannel = in_responseChannel;
    };

    response.prototype.send = function(in_data){
      that._pubClient.publish(that._pipeGuid+':'+this._responseChannel, JSON.stringify(in_data));
    };

    return new response(in_channel);
  },

  _handleRequest : function(in_channel, in_data){
    if(in_channel.indexOf(this._pipeGuid) !== -1){
      if(in_channel.indexOf(':RESPONSE:') === -1){
        var data = JSON.parse(in_data);
        var req = {query: data.query};
        var res = this._response(data.responseChannel);

        this._callbacks[in_channel](req, res);
      }else{
        if(this._callbacks[in_channel]){
          this._callbacks[in_channel](JSON.parse(in_data));
        }
      }
    }
  }
};

exports = module.exports = RedisReqRes;