const AWS = require('aws-sdk');
const ES = require('elasticsearch');
const HTTP_AWS_ES = require('http-aws-es');

class Client {
  checkClient(callback) {
    if (!this.es) {
      callback({
        Error: "Client Not Exist",
        response: "You need to use hostsConfig(config) to config hosts."
      }, null);
      
      return false;
    }
    
    return true;
  }
  
  getClient() {
    this.es = ES.Client({
      hosts: this.hosts,
      connectionClass: HTTP_AWS_ES,
      awsConfig: this.aws,
    });
  }
  
  hostsConfig(config) {
    this.hosts = config;
    this.getClient();
  }
  
  awsConfig(config) {
    this.aws = new AWS.Config({
      credentials: new AWS.Credentials(config.accessKeyId, config.secretAccessKey),
      region: config.region
    });
    if (this.hosts)
      this.getClient();
  }
  
  insert(index, type, id, data, callback) {
    if (this.checkClient(callback)) {
      this.es.create({
        index: index,
        type: type,
        id: id,
        body: data
      }, function (error, response) {
        callback(error, response);
      });
    }
  }
  
  select(index, type, id, fields, callback, caller=null) {
    if (this.checkClient(callback)) {
      this.es.get({
        index: index,
        type: type,
        id: id,
        _source: ((fields) ? fields : true)
      }, function (error, response) {
        if (error)
          callback(error);
        else
          callback(null, response["_source"], caller);
      });
    }
  }
  
  update(index, type, id, data, callback) {
    if (this.checkClient(callback)) {
      this.es.update({
        index: index,
        type: type,
        id: id,
        body: {
          doc: data
        }
      }, function (error, response) {
        callback(error, response);
      });
    }
  }
  
  insertPhoto(user, id, data, callback) {
    function errorHandle(error, response) {
      if (error) {
        callback(error);
      }
    }
    
    this.insert('photos', user, id, data, errorHandle);
    this.select('photos', user, 'indexs', ['tagsIndex'], function (error, response, caller) {
      if (error)
        callback(error);
      else {
        var tagsIndex = new Set(response['tagsIndex']);
        var newIndex = [];
        for (var i=0; i<data['tagsIndex'].length; i++)
          tagsIndex.add(data['tagsIndex'][i]);
        
        tagsIndex.forEach(function(value) {
          newIndex.push(value);
        });
        
        caller.update('photos', user, 'indexs', {
          'tagsIndex': newIndex
        }, function (error, response) {
          callback(error, response);
        });
      }
    }, this);
  }
}

module.exports = Client;
