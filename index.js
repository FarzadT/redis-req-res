var redis = require('redis');
var guid  = require('node-guid');

function RedisReqRes (in_redisConfig) {
  this._pubClient = redis.createClient(in_redisConfig.port, in_redisConfig.host, in_redisConfig.options);
  this._subClient = redis.createClient(in_redisConfig.port, in_redisConfig.host, in_redisConfig.options);

  this._pipeGuid = 'e29a3a25-1fd9-4ade-a2a9-4c3829a87cd5';

  this._myGuid = guid.new();

  this._callbacks = {};

  var exitEvents = ['SIGHUP', 'SIGINT', 'SIGTERM', 'exit', 'uncaughtException'];

  var that = this;
  for(var i in exitEvents){
    process.on(exitEvents[i], function(){
      that._pubClient.srem([that._pipeGuid, that._myGuid]);
    });
  }
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

    var reply = 0;
      
    this.on(responseChannel, function(error, data){
      that._pubClient.scard([this._pipeGuid], function(error, replies){
        reply++;
        if(reply === replies){
          that.off(responseChannel);
        }
      });

      in_callback(error, data);
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
  }
};

exports = module.exports = RedisReqRes;