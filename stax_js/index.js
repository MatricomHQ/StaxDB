const path = require('path');

const staxdbNative = require('./build/Release/staxdb.node');

class StaxResultSet {
    constructor(nativeResultSetHandle) {
        if (!nativeResultSetHandle) {
            throw new Error("StaxResultSet: Native result set handle is null. This indicates a query execution failure.");
        }
        this._resultSet = nativeResultSetHandle;
        this._isGraphResult = this._resultSet.result_type === 1;
    }

    getPage(pageNumber, pageSize) {
        if (this._resultSet === null) throw new Error("Result set has been closed.");
        if (typeof pageNumber !== 'number' || pageNumber < 1) pageNumber = 1;
        if (typeof pageSize !== 'number' || pageSize < 1) pageSize = 50;
        const page = this._resultSet.getPage(pageNumber, pageSize);
        
        return page;
    }

    getTotalCount() {
        if (this._resultSet === null) return 0;
        return this._resultSet.getTotalCount();
    }

    close() {
        if (this._resultSet === null) return;
        this._resultSet.close();
        this._resultSet = null;
    }

    async *[Symbol.asyncIterator]() {
        const pageSize = 50; 
        let currentPageNumber = 1;
        let totalFetched = 0;
        const totalCount = this.getTotalCount();

        try {
            while (totalFetched < totalCount) {
                const page = this.getPage(currentPageNumber, pageSize);
                if (page.results.length === 0) {
                    break; 
                }
                for (const result of page.results) {
                    yield result;
                    totalFetched++;
                }
                currentPageNumber++;
            }
        } finally {
            this.close();
        }
    }
}

class StaxQueryBuilder {
    constructor(graphInstance) {
        this._graph = graphInstance;
        this._plan = [];
    }

    _encodeGeohash(lat, lon, precision = 64) {
        let geohash = 0n;
        let lat_min = -90.0, lat_max = 90.0;
        let lon_min = -180.0, lon_max = 180.0;
        let is_lon = true;

        for (let i = 0; i < precision; i++) {
            geohash <<= 1n;
            if (is_lon) {
                const mid = lon_min + (lon_max - lon_min) / 2.0;
                if (lon > mid) {
                    geohash |= 1n;
                    lon_min = mid;
                } else {
                    lon_max = mid;
                }
            } else {
                const mid = lat_min + (lat_max - lat_min) / 2.0;
                if (lat > mid) {
                    geohash |= 1n;
                    lat_min = mid;
                } else {
                    lat_max = mid;
                }
            }
            is_lon = !is_lon;
        }
        return geohash;
    }

    _processCriterion(opType, field, value) {
        if (typeof value === 'object' && value !== null) {
            
            if (value.hasOwnProperty('gte') || value.hasOwnProperty('lte') || value.hasOwnProperty('gt') || value.hasOwnProperty('lt')) {
                this._plan.push({ op_type: opType, field, value: { gte: value.gte ?? value.gt, lte: value.lte ?? value.lt } });
            } 
            
            else if (value.hasOwnProperty('near') && value.hasOwnProperty('radius')) {
                const { lat, lon } = value.near;
                const radius = value.radius; 

                
                const metersPerDegree = 111320;
                const lat_delta = radius / metersPerDegree;
                const lon_delta = radius / (metersPerDegree * Math.cos(lat * Math.PI / 180));

                const min_lat = lat - lat_delta;
                const max_lat = lat + lat_delta;
                const min_lon = lon - lon_delta;
                const max_lon = lon + lon_delta;

                let gte = this._encodeGeohash(min_lat, min_lon);
                let lte = this._encodeGeohash(max_lat, max_lon);
                
                if (gte > lte) {
                    [gte, lte] = [lte, gte];
                }
                
                this._plan.push({ op_type: opType, field, value: { gte, lte } });
            }
             else if (value.hasOwnProperty('lat') && value.hasOwnProperty('lon')) {
                const geohash = this._encodeGeohash(value.lat, value.lon);
                this._plan.push({ op_type: opType, field, value: { gte: geohash, lte: geohash } });
            }
            else {
                
                this._plan.push({ op_type: opType, field, value });
            }
        } else {
            
            this._plan.push({ op_type: opType, field, value });
        }
    }


    find(criteria) {
        if (Array.isArray(criteria)) {
            
            if (criteria.length === 0) return this;
            
            const processGroup = (op, crit) => {
                const keys = Object.keys(crit);
                if (keys.length > 0) {
                    this._processCriterion(op, keys[0], crit[keys[0]]);
                    for (let i = 1; i < keys.length; i++) {
                        this._processCriterion('intersect', keys[i], crit[keys[i]]);
                    }
                }
            };

            processGroup('find', criteria[0]);
            for (let i = 1; i < criteria.length; i++) {
                processGroup('union', criteria[i]);
            }
        } else if (typeof criteria === 'object' && criteria !== null) {
            
            const keys = Object.keys(criteria);
            if (keys.length === 0) return this;

            this._processCriterion('find', keys[0], criteria[keys[0]]);
            for (let i = 1; i < keys.length; i++) {
                this._processCriterion('intersect', keys[i], criteria[keys[i]]);
            }
        } else {
            throw new TypeError("find() expects an object or an array of objects.");
        }
        return this;
    }

    traverse(direction, relationshipType, filterProperties) {
        if (direction !== 'in' && direction !== 'out') {
            throw new Error("traverse() direction must be 'in' or 'out'.");
        }
        if (typeof relationshipType !== 'string') {
            throw new TypeError("traverse() relationshipType must be a string.");
        }
        const step = { op_type: 'traverse', direction: direction, field: relationshipType };
        if (filterProperties) {
            if (typeof filterProperties !== 'object' || filterProperties === null || Array.isArray(filterProperties)) {
                throw new TypeError("traverse() filterProperties must be an object.");
            }
            step.filter = filterProperties;
        }
        this._plan.push(step);
        return this;
    }

    filter(properties) {
        if (typeof properties !== 'object' || properties === null) {
            throw new TypeError("filter() expects an object of properties.");
        }
        for (const [field, value] of Object.entries(properties)) {
            this._processCriterion('intersect', field, value);
        }
        return this;
    }

    union(properties) {
        if (typeof properties !== 'object' || properties === null) {
            throw new TypeError("union() expects an object of properties.");
        }
        for (const [field, value] of Object.entries(properties)) {
             this._processCriterion('union', field, value);
        }
        return this;
    }
    
    execute() {
        return this._graph.executeQuery(this._plan);
    }
}

class StaxGraphTransaction {
    constructor(nativeTxnHandle) {
        this._handle = nativeTxnHandle;
    }
    insertObject(dataObject) { return this._handle.insertObject(dataObject); }
    updateObject(objectId, dataObject) { this._handle.updateObject(objectId, dataObject); }
    deleteObject(objectId) { this._handle.deleteObject(objectId); }
    insertRelationship(sourceId, relType, targetId) { this._handle.insertRelationship(sourceId, relType, targetId); }
    commit() { this._handle.commit(); }
    abort() { this._handle.abort(); }
}

class StaxGraph {
    constructor(nativeGraphHandle) {
        this._handle = nativeGraphHandle;
        this._planCache = new Map();
    }
    
    beginTransaction() {
        const nativeTxnHandle = this._handle.beginTransaction();
        return new StaxGraphTransaction(nativeTxnHandle);
    }

    insertRelationship(sourceId, relType, targetId) {
        this._handle.insertRelationship(sourceId, relType, targetId);
    }

    deleteObject(objectId) {
        this._handle.deleteObject(objectId);
    }

    insertObject(dataObject) {
        if (typeof dataObject !== 'object' || dataObject === null || Array.isArray(dataObject)) {
            throw new TypeError("insertObject expects the argument 'dataObject' to be an object.");
        }
        return this._handle.insertObject(dataObject);
    }

    updateObject(objectId, dataObject) {
        if (typeof objectId !== 'number') {
            throw new TypeError("updateObject expects the first argument 'objectId' to be a number.");
        }
        if (typeof dataObject !== 'object' || dataObject === null || Array.isArray(dataObject)) {
            throw new TypeError("updateObject expects the second argument 'dataObject' to be an object.");
        }
        this._handle.updateObject(objectId, dataObject);
    }


    commit() {
        this._handle.commit();
    }

    query() {
        return new StaxQueryBuilder(this);
    }

    executeQuery(plan) {
        if (!Array.isArray(plan)) {
            throw new Error("executeQuery requires a query plan (array of step objects).");
        }
        
        const signature = JSON.stringify(plan.map(step => {
            const { value, filter, ...sigStep } = step;
            return sigStep;
        }));
        
        let planId = this._planCache.get(signature);

        if (planId === undefined) {
            planId = this._handle.compileQuery(plan);
            if(planId === -1 || planId === 4294967295) {
                 throw new Error("Failed to compile query plan in native layer.");
            }
            this._planCache.set(signature, planId);
        }
        
        const nativeResultSetHandle = this._handle.executeQuery(planId, plan);
        return new StaxResultSet(nativeResultSetHandle);
    }
}


class StaxReadTransaction {
    constructor(nativeTxnHandle) {
        this._txn = nativeTxnHandle;
    }

    get(key) {
        if (!this._txn) throw new Error("Transaction is already closed.");
        const buffer = this._txn.get(key);
        return buffer === null ? undefined : buffer;
    }

    commit() {
        if (!this._txn) return;
        this._txn.commit();
        this._txn = null;
    }

    abort() {
        if (!this._txn) return;
        this._txn.abort();
        this._txn = null;
    }
}

class StaxWriteTransaction {
    constructor(nativeTxnHandle) {
        this._handle = nativeTxnHandle;
    }
    insert(key, value) { this._handle.insert(key, value); }
    remove(key) { this._handle.remove(key); }
    commit() { this._handle.commit(); }
    abort() { this._handle.abort(); }
}

class StaxCollection {
    constructor(collectionHandle, dbInstance) {
        this._handle = collectionHandle;
        this._dbInstance = dbInstance;
    }

    executeBatch(operations) {
        if (!Array.isArray(operations)) {
            throw new Error("executeBatch expects an array of operations.");
        }
        if (operations.length === 0) return;
        
        let totalSize = 4;
        for (const op of operations) {
            totalSize += 1; 
            totalSize += 4 + Buffer.byteLength(op.key, 'utf8'); 
            if (op.type === 'insert') {
                totalSize += 4 + Buffer.byteLength(op.value, 'utf8');
            }
        }

        const buffer = Buffer.allocUnsafe(totalSize);
        let offset = 0;
        buffer.writeUInt32LE(operations.length, offset);
        offset += 4;
        for (const op of operations) {
            if (op.type === 'insert') {
                buffer.writeUInt8(1, offset);
                offset += 1;
                const keyWritten = buffer.write(op.key, offset + 4, 'utf8');
                buffer.writeUInt32LE(keyWritten, offset);
                offset += 4 + keyWritten;
                const valueWritten = buffer.write(op.value, offset + 4, 'utf8');
                buffer.writeUInt32LE(valueWritten, offset);
                offset += 4 + valueWritten;
            } else if (op.type === 'remove') {
                buffer.writeUInt8(2, offset);
                offset += 1;
                const keyWritten = buffer.write(op.key, offset + 4, 'utf8');
                buffer.writeUInt32LE(keyWritten, offset);
                offset += 4 + keyWritten;
            }
        }
        this._dbInstance.executeBatch(this._handle, buffer);
    }
    
    insertSync(key, value) { this.executeBatch([{ type: 'insert', key, value }]); }
    removeSync(key) { this.executeBatch([{ type: 'remove', key }]); }

    getSync(key) {
        const txn = this.beginTransaction(true);
        try { return txn.get(key); } finally { txn.commit(); }
    }

    beginTransaction(isReadOnly = true) {
        const nativeTxnHandle = this._dbInstance.beginTransaction(this._handle, isReadOnly);
        if (isReadOnly) {
            return new StaxReadTransaction(nativeTxnHandle);
        } else {
            return new StaxWriteTransaction(nativeTxnHandle);
        }
    }

    range(options = {}) {
        if (!this._dbInstance) throw new Error("Collection is not attached to an open database.");
        const nativeResultSetHandle = this._dbInstance.executeRangeQuery(this._handle, options);
        return new StaxResultSet(nativeResultSetHandle);
    }
}

class StaxDB {
    constructor(dbPath, options = {}) {
        if (!dbPath) throw new Error("StaxDB constructor requires a database path.");
        const absolutePath = path.resolve(dbPath);
        this._dbInstance = new staxdbNative.StaxDB(absolutePath, options);
        this._collections = new Map();
        this._graph = null;
    }

    closeSync() {
        if (!this._dbInstance) return;
        this._dbInstance.closeSync();
        this._dbInstance = null;
        this._collections.clear();
        this._graph = null;
    }
    
    
    static dropSync(dbPath) {
        if (!dbPath) throw new Error("StaxDB.dropSync requires a database path.");
        const absolutePath = path.resolve(dbPath);
        
        staxdbNative.StaxDB.dropSync(absolutePath);
    }

    getCollectionSync(collectionName) {
        if (!this._dbInstance) throw new Error("Database is not open.");
        if (this._collections.has(collectionName)) return this._collections.get(collectionName);
        const collectionHandle = this._dbInstance.getCollectionSync(collectionName);
        const collection = new StaxCollection(collectionHandle, this._dbInstance);
        this._collections.set(collectionName, collection);
        return collection;
    }

    getGraph() {
        if (!this._dbInstance) throw new Error("Database is not open.");
        if (!this._graph) {
            const nativeGraphHandle = this._dbInstance.getGraph();
            this._graph = new StaxGraph(nativeGraphHandle);
        }
        return this._graph;
    }
}

class StaxDBAsync {
    constructor() { 
        this._db = null; 
    }
    
    async open(dbPath, options = {}) {
        if (this._db) throw new Error("Async DB is already open.");
        
        return new Promise((resolve, reject) => {
            try {
                this._db = new StaxDB(dbPath, options);
                resolve();
            } catch (err) {
                reject(err);
            }
        });
    }

    async close() {
        if (!this._db) return;
        return new Promise((resolve) => {
            this._db.closeSync();
            this._db = null;
            resolve();
        });
    }

    async insertBatchAsync(batch) {
        if (!this._db || !this._db._dbInstance) throw new Error("Async DB is not open.");
        if (!Array.isArray(batch) || batch.length === 0) return;
        const collection = this._db.getCollectionSync("async_batch_collection"); 
        
        return new Promise((resolve, reject) => {
            this._db._dbInstance.insertBatchAsync(collection._handle, batch, (err) => {
                if (err) return reject(new Error(err));
                resolve();
            });
        });
    }
    
    async multiGetAsync(keys) {
        if (!this._db || !this._db._dbInstance) throw new Error("Async DB is not open.");
        if (!Array.isArray(keys) || keys.length === 0) return [];
        const collection = this._db.getCollectionSync("async_multiget_collection");

        return new Promise((resolve, reject) => {
            this._db._dbInstance.multiGetAsync(collection._handle, keys, (err, results) => {
                if (err) return reject(new Error(err));
                resolve(results);
            });
        });
    }
}

module.exports = { StaxDB, StaxDBAsync, StaxCollection, StaxReadTransaction, StaxWriteTransaction, StaxResultSet, StaxGraph, StaxGraphTransaction };