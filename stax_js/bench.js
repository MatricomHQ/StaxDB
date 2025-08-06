const fs = require('fs');
const path = require('path');
const { StaxDB } = require('./index.js');
const lmdb = require('lmdb');

const DB_PATH_STAX = './staxdb_bench_data';
const DB_PATH_LMDB = './lmdb_bench_data';

const NUM_POINT_OPS = 1000;
const NUM_BATCH_ITEMS = 50000;
const NUM_GRAPH_OBJECTS = 10000;

const results = [];


const time = (fn) => {
    const start = process.hrtime.bigint();
    fn();
    const end = process.hrtime.bigint();
    return end - start;
};


const timeAsync = async (fn) => {
    const start = process.hrtime.bigint();
    await fn();
    const end = process.hrtime.bigint();
    return end - start;
}

const formatResults = () => {
    const nameWidth = 45, opsWidth = 15, timeWidth = 20, latWidth = 20, opsSecWidth = 15;
    const totalWidth = nameWidth + opsWidth + timeWidth + latWidth + opsSecWidth + 11;

    console.log(`\n╔${'═'.repeat(totalWidth-2)}╗`);
    console.log(`║${' StaxDB vs LMDB - Micro-Benchmark Suite Results'.padEnd(totalWidth-2)}║`);
    console.log(`╠${'═'.repeat(nameWidth+1)}╦${'═'.repeat(opsWidth+1)}╦${'═'.repeat(timeWidth+1)}╦${'═'.repeat(latWidth+1)}╦${'═'.repeat(opsSecWidth+1)}╣`);
    let header = `║ ${'Benchmark'.padEnd(nameWidth)}│ ${'Total Ops'.padEnd(opsWidth)}│ ${'Total Time (ms)'.padEnd(timeWidth)}│ ${'Avg Latency (ms)'.padEnd(latWidth)}│ ${'Ops/sec'.padEnd(opsSecWidth)}║`;
    console.log(header);
    console.log(`╠${'═'.repeat(nameWidth+1)}╬${'═'.repeat(opsWidth+1)}╬${'═'.repeat(timeWidth+1)}╬${'═'.repeat(latWidth+1)}╬${'═'.repeat(opsSecWidth+1)}╣`);

    const groupedResults = new Map();
    results.forEach(r => {
        const shortName = r.name
            .replace(' (Point by Numeric Index)', ' (Point Idx)')
            .replace(' (Multi-Filter/Range)', ' (Multi-Filter)')
            .replace(' (Multi-Hop Traverse)', ' (Multi-Hop)');
        const key = `${shortName} - ${r.engine}`;
        groupedResults.set(key, r);
    });

    for (const [key, r] of groupedResults.entries()) {
        if (r.impractical) {
             let row = `║ ${key.padEnd(nameWidth)}│ ${'N/A'.padEnd(opsWidth)}│ ${'N/A'.padEnd(timeWidth)}│ ${'N/A'.padEnd(latWidth)}│ ${'N/A'.padEnd(opsSecWidth)}║`;
             console.log(row);
        } else {
            const timeMs = (Number(r.total_ns) / 1e6).toFixed(3);
            const avgMs = (Number(r.total_ns) / r.ops / 1e6).toFixed(4);
            const opsPerSec = (r.ops / (Number(r.total_ns) / 1e9)).toLocaleString(undefined, { maximumFractionDigits: 0 });
            let row = `║ ${key.padEnd(nameWidth)}│ ${r.ops.toLocaleString().padEnd(opsWidth)}│ ${timeMs.padEnd(timeWidth)}│ ${avgMs.padEnd(latWidth)}│ ${opsPerSec.padEnd(opsSecWidth)}║`;
            console.log(row);
        }
    }
    console.log(`╚${'═'.repeat(nameWidth+1)}╩${'═'.repeat(opsWidth+1)}╩${'═'.repeat(timeWidth+1)}╩${'═'.repeat(latWidth+1)}╩${'═'.repeat(opsSecWidth+1)}╝`);
    
    
    const compNameWidth = 46, latCompWidth = 38;
    const compTotalWidth = compNameWidth + latCompWidth + 4;
    console.log(`\n╔${'═'.repeat(compTotalWidth-2)}╗`);
    console.log(`║${' Performance Comparison Summary'.padEnd(compTotalWidth-2)}║`);
    console.log(`╠${'═'.repeat(nameWidth+1)}╦${'═'.repeat(latCompWidth+1)}╣`);
    let compHeader = `║ ${'Benchmark'.padEnd(nameWidth)}│ ${'StaxDB vs LMDB Latency'.padEnd(latCompWidth)}║`;
    console.log(compHeader);
    console.log(`╠${'═'.repeat(nameWidth+1)}╬${'═'.repeat(latCompWidth+1)}╣`);
    
    const benchNames = [...new Set(results.map(r => r.name))];
    for(const name of benchNames) {
        const staxResult = results.find(r => r.name === name && r.engine === 'StaxDB');
        const lmdbResult = results.find(r => r.name === name && r.engine === 'LMDB');
        
        if (staxResult && lmdbResult && !lmdbResult.impractical) {
            const staxAvgMs = Number(staxResult.total_ns) / staxResult.ops / 1e6;
            const lmdbAvgMs = Number(lmdbResult.total_ns) / lmdbResult.ops / 1e6;
            const latencyDiff = ((staxAvgMs / lmdbAvgMs) - 1) * 100;
            const fasterSlower = latencyDiff < 0 ? `${(-latencyDiff).toFixed(1)}% Faster` : `${latencyDiff.toFixed(1)}% Slower`;
            let row = `║ ${staxResult.name.padEnd(nameWidth)}│ ${fasterSlower.padEnd(latCompWidth)}║`;
            console.log(row);
        }
    }
    console.log(`╚${'═'.repeat(nameWidth+1)}╩${'═'.repeat(latCompWidth+1)}╝`);
};


const cleanup = () => {
    for (const p of [DB_PATH_STAX, DB_PATH_LMDB]) {
        if (fs.existsSync(p)) {
            fs.rmSync(p, { recursive: true, force: true });
        }
    }
};

const setupDb = (name) => {
    cleanup();
    console.log(`\n--- Running: ${name} ---`);
    const staxDb = new StaxDB(DB_PATH_STAX);
    const lmdbDb = lmdb.open({ path: DB_PATH_LMDB, compression: false });
    return { staxDb, lmdbDb };
};



function bench_kv_point_ops() {
    const { staxDb, lmdbDb } = setupDb('KV Point Operations');
    const collection = staxDb.getCollectionSync('kv_point');
    const keys = Array.from({ length: NUM_POINT_OPS }, (_, i) => `user:${i}`);
    const value = 'some_user_payload_data_that_is_long_enough';

    
    const staxInsertTime = time(() => { for (const key of keys) { collection.insertSync(key, value); } });
    results.push({ name: 'KV Insert (Point)', engine: 'StaxDB', ops: NUM_POINT_OPS, total_ns: staxInsertTime });
    const staxGetTime = time(() => { for (const key of keys) { collection.getSync(key); } });
    results.push({ name: 'KV Get (Point)', engine: 'StaxDB', ops: NUM_POINT_OPS, total_ns: staxGetTime });
    
    
    const lmdbInsertTime = time(() => { 
        for (const key of keys) { 
            lmdbDb.transactionSync(() => {
                lmdbDb.putSync(key, value); 
            });
        } 
    });
    results.push({ name: 'KV Insert (Point)', engine: 'LMDB', ops: NUM_POINT_OPS, total_ns: lmdbInsertTime });
    const lmdbGetTime = time(() => { for (const key of keys) { lmdbDb.get(key); } });
    results.push({ name: 'KV Get (Point)', engine: 'LMDB', ops: NUM_POINT_OPS, total_ns: lmdbGetTime });

    staxDb.closeSync();
    lmdbDb.close();
}

async function bench_kv_batch_ops() {
    const { staxDb, lmdbDb } = setupDb('KV Batch Operations');
    const collection = staxDb.getCollectionSync('kv_batch');
    const ops = Array.from({ length: NUM_BATCH_ITEMS }, (_, i) => ({ type: 'insert', key: `user:${i}`, value: 'some_user_payload_data' }));
    
    
    const staxBatchInsertTime = await timeAsync(async () => { await collection.executeBatch(ops); });
    results.push({ name: 'KV Insert (Batch)', engine: 'StaxDB', ops: NUM_BATCH_ITEMS, total_ns: staxBatchInsertTime });
    
    
    const lmdbBatchInsertTime = time(() => { lmdbDb.transactionSync(() => { for (const op of ops) { lmdbDb.putSync(op.key, op.value); } }); });
    results.push({ name: 'KV Insert (Batch)', engine: 'LMDB', ops: NUM_BATCH_ITEMS, total_ns: lmdbBatchInsertTime });

    staxDb.closeSync();
    lmdbDb.close();
}

async function bench_kv_range_scan() {
    const { staxDb, lmdbDb } = setupDb('KV Range Scan');
    const collection = staxDb.getCollectionSync('kv_range');
    const ops = Array.from({ length: NUM_BATCH_ITEMS }, (_, i) => ({ type: 'insert', key: `user:${i.toString().padStart(10, '0')}`, value: 'data' }));
    
    await collection.executeBatch(ops);
    lmdbDb.transactionSync(() => { for (const op of ops) { lmdbDb.putSync(op.key, op.value); } });

    
    const staxRangeTime = time(() => { const rs = collection.range({ start: 'user:', end: 'user:~' }); rs.close(); });
    results.push({ name: 'KV Range Query', engine: 'StaxDB', ops: 1, total_ns: staxRangeTime });

    
    const lmdbRangeTime = time(() => { for (const {} of lmdbDb.getRange({ start: 'user:', end: 'user:~' })) {} });
    results.push({ name: 'KV Range Query', engine: 'LMDB', ops: 1, total_ns: lmdbRangeTime });

    staxDb.closeSync();
    lmdbDb.close();
}

function bench_graph_ingestion() {
    const { staxDb } = setupDb('Graph Ingestion');
    const graph = staxDb.getGraph();

    const graphObjects = Array.from({ length: NUM_GRAPH_OBJECTS }, (_, i) => ({
        id: i,
        type: 'user',
        name: `User_${i}`,
        status: i % 100 === 0 ? 'premium' : 'standard',
        age: 20 + (i % 50)
    }));
    const relationships = Array.from({ length: NUM_GRAPH_OBJECTS }, (_, i) => (i > 10 ? { from: i, to: i - (i % 10) } : null)).filter(Boolean);

    const objectIds = new Map();

    const ingestionFullObjectTime = time(() => {
        for (const item of graphObjects) {
            const { id, ...facts } = item;
            const newId = graph.insertObject(facts);
            objectIds.set(id, newId);
        }
        for (const rel of relationships) {
            const fromId = objectIds.get(rel.from);
            const toId = objectIds.get(rel.to);
            if (fromId !== undefined && toId !== undefined) {
                graph.insertRelationship(fromId, 'FOLLOWS', toId);
            }
        }
        graph.commit();
    });
    results.push({ name: 'Graph Ingestion (Full Object)', engine: 'StaxDB', ops: NUM_GRAPH_OBJECTS + relationships.length, total_ns: ingestionFullObjectTime });

    results.push({ name: 'Graph Ingestion (Full Object)', engine: 'LMDB', impractical: true });

    staxDb.closeSync();
}


function bench_graph_queries() {
    const { staxDb, lmdbDb } = setupDb('Graph Queries');
    const graph = staxDb.getGraph();
    const COMMIT_INTERVAL = 10000;

    console.log('  Setting up data for graph query benchmarks...');
    for (let i = 0; i < NUM_BATCH_ITEMS; i++) {
        const userObject = {
            type: 'user',
            age: 20 + (i % 50),
            status: i % 100 === 0 ? 'premium' : 'standard'
        };
        const newId = graph.insertObject(userObject);
        
        if (i > 10) {
            const targetId = newId - (i % 10);
            if(targetId > 0) {
                 graph.insertRelationship(newId, 'FOLLOWS', targetId);
            }
        }

        if ((i + 1) % COMMIT_INTERVAL === 0) {
            graph.commit();
        }
    }
    graph.commit();
    console.log('  Setup complete.');

    
    const pointQueryTime = time(() => { for (let i = 0; i < NUM_POINT_OPS; i++) { const rs = graph.executeQuery([{ op_type: 'find', field: 'age', value: { gte: 25, lte: 25 } }]); rs.close(); } });
    results.push({ name: 'Graph Query (Point by Numeric Index)', engine: 'StaxDB', ops: NUM_POINT_OPS, total_ns: pointQueryTime });
    results.push({ name: 'Graph Query (Point by Numeric Index)', engine: 'LMDB', impractical: true });

    
    const multiFilterTime = time(() => { for (let i = 0; i < NUM_POINT_OPS; i++) { const rs = graph.executeQuery([{ op_type: 'find', field: 'status', value: 'premium' },{ op_type: 'intersect', field: 'age', value: { gte: 30, lte: 40 } }]); rs.close(); } });
    results.push({ name: 'Graph Query (Multi-Filter/Range)', engine: 'StaxDB', ops: NUM_POINT_OPS, total_ns: multiFilterTime });
    results.push({ name: 'Graph Query (Multi-Filter/Range)', engine: 'LMDB', impractical: true });
    
    
    const multiHopTime = time(() => { for (let i = 0; i < NUM_POINT_OPS; i++) { const rs = graph.executeQuery([{ op_type: 'find', field: 'status', value: 'premium' },{ op_type: 'traverse', direction: 'out', field: 'FOLLOWS' }]); rs.close(); } });
    results.push({ name: 'Graph Query (Multi-Hop Traverse)', engine: 'StaxDB', ops: NUM_POINT_OPS, total_ns: multiHopTime });
    results.push({ name: 'Graph Query (Multi-Hop Traverse)', engine: 'LMDB', impractical: true });
    
    staxDb.closeSync();
    lmdbDb.close();
}

async function runAllBenchmarks() {
    console.log(`Starting benchmarks with ${NUM_POINT_OPS} point ops and ${NUM_BATCH_ITEMS} batch ops.`);
    
    bench_kv_point_ops();
    await bench_kv_batch_ops();
    await bench_kv_range_scan();
    bench_graph_ingestion();
    bench_graph_queries();

    formatResults();
    cleanup();
}

runAllBenchmarks();