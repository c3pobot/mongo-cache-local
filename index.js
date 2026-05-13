const log = require('./logger')

const { MongoClient } = require("mongodb");

module.exports = class MongoCache {
  constructor({ connection_string, db_name, collections }){
    this._mongo = new MongoClient(connection_string)
    this._mongo_ready = false, this._collections = collections || [],  this._dbo = mongo.db(db_name || 'mongo_cache')
  }
  async _init(){
    try{
      await this._mongo.connect()
      let status = await this._mongo.db('admin').command({ ping: 1 })
      if(status.ok > 0){
        this._mongo_ready = true
        log.info(`mongo connection successful...`)
        return this._createTables()
      }
      setTimeout(init, 5000)
    }catch(e){
      log.error(e)
      setTimeout(this._init, 5000)
    }
  }
  async _createTables(){
    try{
      if(!this._collections || this._collections?.length == 0){
        this._mongo_ready = true
        return
      }
      for(let i in this._collections){
        if(this._collections[i].expireSeconds) await this._dbo.collection(this._collections[i].name)?.createIndex({ TTL: 1 }, { expireAfterSeconds: this._collections[i].expireSeconds )
      }
      this._mongo_ready = true
      return
      setTimeout(this._createTables, 5000)
    }catch(e){
      log.error(e)
      setTimeout(this._createTables, 5000)
    }
  }

  async aggregate( collection, matchCondition, data = []){
    try{
      if(matchCondition) data.unshift({$match: matchCondition})
      return await this._dbo.collection(collection).aggregate(data, { allowDiskUse: true }).toArray()
    }catch(e){
      log.error(e)
    }
  }
  async all( collection, matchCondition, project ){
    try{
      return await this._dbo.collection( collection ).find( matchCondition, { projection: project } ).toArray()
    }catch(e){
      log.error(e)
    }
  }
  async del( collection, matchCondition ){
    try{
      return await this._dbo.collection(collection).deleteOne(matchCondition)
    }catch(e){
      log.error(e)
    }
  }
  async delMany( collection, matchCondition ){
    try{
      return await this._dbo.collection(collection).deleteMany(matchCondition)
    }catch(e){
      log.error(e)
    }
  }
  async count( collection, matchCondition ){
    try{
      return await this._dbo.collection( collection ).countDocuments(matchCondition)
    }catch(e){
      log.error(e)
    }
  }
  async createIndex(collection, indexObj, opts = {}){
    try{
      if(!indexObj) throw('No index provided...')
      //opts = { background: true, expireAfterSeconds: 600 }
      return await this._dbo.collection( collection ).createIndex(indexObj, opts)
    }catch(e){
      log.error(e)
    }
  }

  async get(collection, matchCondition, project){
    try{
      let res = await this._dbo.collection( collection ).find( matchCondition, { projection: project } ).toArray()
      if(res?.length > 0) return res[0]
    }catch(e){
      log.error(e)
    }
  }

  async listIndexes( collection ){
    try{
      return await this._dbo.collection( collection ).listIndexes().toArray()
    }catch(e){
      log.error(e)
    }
  }

  async limit( collection, matchCondition, project, limitCount = 50 ){
    try{
      return await this._dbo.collection( collection ).find( matchCondition, { projection: project } ).limit( limitCount ).toArray()
    }catch(e){
      log.error(e)
    }
  }

  async set( collection, matchCondition, data ){
    try{
      if(!data || !matchCondition || !collection) return
      if(!data?.TTL) data.TTL = new Date()
      let res = await this._dbo.collection( collection ).updateOne( matchCondition, { $set: data }, { upsert: true } )
      delete data.TTL
      return res?.acknowledged
    }catch(e){
      log.error(e)
    }
  }
  status(){
    return this._mongo_ready
  }
}
