const AWS = require('aws-sdk');
const ES = require('elasticsearch');
const HTTP_AWS_ES = require('http-aws-es');

class Client {
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
    if (!this.es) {
      callback({
        Error: "Client Not Exist",
        response: "You need to use hostsConfig(config) to config hosts."
      }, null);
      return;
    }
    
    this.es.create({
      index: index,
      type: type,
      id: id,
      body: data
    }, function (error ,response) {
      callback(error, response);
    });
  }
}

module.exports = Client;
