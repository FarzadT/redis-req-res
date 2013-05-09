var redis = require('redis');
var guid  = require('node-guid');

function RedisReqRes (in_redisConfig) {
  this._pubClient = redis.createClient(in_redisConfig.port, in_redisConfig.host, in_redisConfig.options);
  this._subClient = redis.createClient(in_redisConfig.port, in_redisConfig.host, in_redisConfig.options);

  this._pipeGuid = 'e29a3a25-1fd9-4ade-a2a9-4c3829a87cd5';

  this._myGuid = guid.new();

  this._callbacks = {};

  // List of intervals mapped to a specific channel.
  // NOTE: These intervals are used to set an expiration date on the keys
  //       added to the set.
  this._intervals = {};

  var that = this;
  // Clean up the key entry.
  this._setExpiration(this._pipeGuid);
  setInterval(function(){
    that._setExpiration(that._pipeGuid);
  },4*60*1000);
};

RedisReqRes.prototype = {
  init: function(in_callback){
    var that = this;
    
    in_callback = in_callback || function(){};

    this._subClient.on("connect", function(){
      that._subClient.on("message",function(channel, message){
        that._handleRequest(channel, message);
      });

      that._pubClient.sadd([that._pipeGuid, that._myGuid]);

      in_callback();
    });

    this._subClient.on("error",function(err){
      throw err;
    });
  },

  on: function(in_channel, in_callback){
    var that = this;

    var channel = this._pipeGuid+':'+in_channel;

    this._subClient.subscribe(channel);

    that._pubClient.sadd([channel, this._myGuid]);


    this._callbacks[channel] = in_callback;

    that._setExpiration(channel);
    this._intervals[channel] = setInterval(function(){
      that._setExpiration(channel);
    },4*60*1000);
  },

  off: function(in_channel){
    var channel = this._pipeGuid+':'+in_channel;
    
    delete this._callbacks[channel];

    this._pubClient.srem([channel, this._myGuid]);

    clearInterval(this._intervals[channel]);
    delete this._intervals[channel];

    this._subClient.unsubscribe(channel);
  },

  /*
  * Send an arbitrary message without waiting for a response.
  */
  sendMessage: function(in_channel, in_data, in_error){
    var data = {data: in_data, error: error};
    this._pubClient.publish(this._pipeGuid + ':' + in_channel, JSON.stringify(data));
  },

  request: function(in_channel, in_data, in_callback, in_returnFirstResponse){
    in_data = in_data || {};

    var responseChannel = 'RESPONSE:' + guid.new();

    var data = {query: in_data, responseChannel: responseChannel};

    var that = this;

    var reply = 0;
      
    this.on(responseChannel, function(error, data){
      that._pubClient.scard([requestChannel], function(error, replies){
        reply++;
        if(reply === replies){
          that.off(responseChannel);
        }
      });

      in_callback(error, data);

      if(in_returnFirstResponse){
        that.off(responseChannel);
      }
    });

    var requestChannel = this._pipeGuid + ':' + in_channel;
    this._pubClient.publish(requestChannel, JSON.stringify(data));
  },

  _response: function(in_channel){
    var that = this;
    function response(in_responseChannel){
      this._responseChannel = in_responseChannel;
    };

    response.prototype.send = function(error, in_data){
      var data = {data: in_data, error: error};
      that._pubClient.publish(that._pipeGuid+':'+this._responseChannel, JSON.stringify(data));
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
          var data = JSON.parse(in_data);
          this._callbacks[in_channel](data.error, data.data);
        }
      }
    }
  },

  _setExpiration: function(key){
    this._pubClient.expire([key, 5*60], function(error, res){
      if(error){
        console.error('Redis-Req-Res ERROR: ',error);
      }
    });
  }
};

exports = module.exports = RedisReqRes;