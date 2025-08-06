const fs = require('fs');
const path = require('path');
const { Worker, isMainThread, workerData, parentPort } = require('worker_threads');
const { StaxDBAsync } = require('./index.js');

const DB_PATH_STAX = './staxdb_profile_data';
const NUM_ITEMS = 500_000;

const NUM_THREADS = isMainThread ? (require('os').cpus().length > 1 ? 2 : 1) : 1;
const ITEMS_PER_THREAD = Math.floor(NUM_ITEMS / NUM_THREADS);


function generateKey(index) {
    return `user:${index.toString().padStart(10, '0')}`;
}

function generateValue(index) {
    return `profile_data_for_user_${index}_with_some_extra_padding_to_simulate_real_world_data`;
}

function generateProfileData(count) {
    const data = [];
    for (let i = 0; i < count; i++) {
        data.push({
            key: generateKey(i),
            value: generateValue(i)
        });
    }
    return data;
}

async function workerLogic() {
    const { dbPath, data } = workerData;
    const timings = [];
    const db = new StaxDBAsync();
    await db.open(dbPath);

    
    let s_start, s_end, ffi_start, ffi_end, total_start, total_end;
    
    total_start = performance.now();

    
    s_start = performance.now();
    const batch = data;
    const batchLength = batch.length;
    const LEN_FIELD_SIZE = 4;
    let totalPayloadSize = 0;
    for (const op of batch) {
        totalPayloadSize += Buffer.byteLength(op.key, 'utf8');
        totalPayloadSize += Buffer.byteLength(op.value, 'utf8');
    }
    const totalRequestBufferSize = 
        LEN_FIELD_SIZE + (batchLength * (LEN_FIELD_SIZE + LEN_FIELD_SIZE)) + totalPayloadSize;
    const requestBuffer = Buffer.allocUnsafe(totalRequestBufferSize);
    let currentOffset = 0;
    requestBuffer.writeUInt32LE(batchLength, currentOffset);
    currentOffset += LEN_FIELD_SIZE;
    for (const op of batch) {
        const keyLen = requestBuffer.write(op.key, currentOffset + LEN_FIELD_SIZE, 'utf8');
        requestBuffer.writeUInt32LE(keyLen, currentOffset);
        currentOffset += LEN_FIELD_SIZE + keyLen;
        const valLen = requestBuffer.write(op.value, currentOffset + LEN_FIELD_SIZE, 'utf8');
        requestBuffer.writeUInt32LE(valLen, currentOffset);
        currentOffset += LEN_FIELD_SIZE + valLen;
    }
    s_end = performance.now();
    timings.push({ stage: 'Insert: JS Serialization', time_ms: s_end - s_start });
    
    
    ffi_start = performance.now();
    await db.insertBatchAsync(batch);
    ffi_end = performance.now();
    timings.push({ stage: 'Insert: FFI + C++ Exec', time_ms: ffi_end - ffi_start });
    
    total_end = performance.now();
    timings.push({ stage: 'Insert: Total E2E', time_ms: total_end - total_start, isTotal: true });

    
    total_start = performance.now();
    const keysToGet = data.map(d => d.key);
    
    
    s_start = performance.now();
    const getBatchLength = keysToGet.length;
    let totalKeysPayloadSize = 0;
    for (const key of keysToGet) {
        totalKeysPayloadSize += Buffer.byteLength(key, 'utf8');
    }
    const getRequestBuffer = Buffer.allocUnsafe((getBatchLength * LEN_FIELD_SIZE) + totalKeysPayloadSize);
    let getKeyOffset = 0;
    for (const key of keysToGet) {
        const keyByteLength = Buffer.byteLength(key, 'utf8');
        getRequestBuffer.writeUInt32LE(keyByteLength, getKeyOffset);
        getKeyOffset += LEN_FIELD_SIZE;
        getRequestBuffer.write(key, getKeyOffset, keyByteLength, 'utf8');
        getKeyOffset += keyByteLength;
    }
    s_end = performance.now();
    timings.push({ stage: 'Get: JS Serialization', time_ms: s_end - s_start });

    
    ffi_start = performance.now();
    const responseBufferArray = await db.multiGetAsync(keysToGet);
    ffi_end = performance.now();
    timings.push({ stage: 'Get: FFI + C++ Exec', time_ms: ffi_end - ffi_start });

    
    s_start = performance.now();
    const results = responseBufferArray;
    if (results.length !== getBatchLength) {
        console.error(`Result length mismatch! Expected ${getBatchLength}, got ${results.length}`);
    }
    s_end = performance.now();
    timings.push({ stage: 'Get: JS Deserialization', time_ms: s_end - s_start });

    total_end = performance.now();
    timings.push({ stage: 'Get: Total E2E', time_ms: total_end - total_start, isTotal: true });

    
    await db.close();
    parentPort.postMessage(timings);
}


async function main() {
    console.log(`--- StaxDB E2E Performance Profiler (Async/Parallel) ---`);
    console.log(`(${NUM_ITEMS.toLocaleString()} items across ${NUM_THREADS} threads)`);

    
    if (fs.existsSync(DB_PATH_STAX)) {
        fs.rmSync(DB_PATH_STAX, { recursive: true, force: true });
    }
    fs.mkdirSync(DB_PATH_STAX, { recursive: true });

    console.log('Generating base data on main thread...');
    const allData = generateProfileData(NUM_ITEMS);
    console.log('Data generation complete.');

    const workerPromises = [];
    const allWorkerTimings = [];

    console.log('Starting worker threads for profiling...');
    for (let i = 0; i < NUM_THREADS; i++) {
        const start = i * ITEMS_PER_THREAD;
        const end = start + ITEMS_PER_THREAD;
        const workerDataChunk = allData.slice(start, end);

        workerPromises.push(new Promise((resolve, reject) => {
            const worker = new Worker(__filename, {
                workerData: {
                    dbPath: DB_PATH_STAX,
                    data: workerDataChunk,
                }
            });
            worker.on('message', (timings) => {
                allWorkerTimings.push(timings);
                resolve();
            });
            worker.on('error', reject);
            worker.on('exit', (code) => {
                if (code !== 0) reject(new Error(`Worker stopped with exit code ${code}`));
            });
        }));
    }

    await Promise.all(workerPromises);
    console.log('All worker threads finished.');

    
    const aggregatedTimings = new Map();
    for (const workerTimings of allWorkerTimings) {
        for (const timing of workerTimings) {
            if (!aggregatedTimings.has(timing.stage)) {
                aggregatedTimings.set(timing.stage, { time_ms: 0, isTotal: timing.isTotal });
            }
            aggregatedTimings.get(timing.stage).time_ms += timing.time_ms;
        }
    }

    const finalAveragedTimings = [];
    aggregatedTimings.forEach((value, key) => {
        finalAveragedTimings.push({
            stage: key,
            time_ms: value.time_ms / NUM_THREADS, 
            isTotal: value.isTotal,
        });
    });

    
    finalAveragedTimings.sort((a, b) => {
        const order = ['Insert:', 'Get:'];
        const aOrder = a.stage.includes(order[0]) ? 0 : 1;
        const bOrder = b.stage.includes(order[0]) ? 0 : 1;
        if (aOrder !== bOrder) return aOrder - bOrder;
        if (a.isTotal) return 1;
        if (b.isTotal) return -1;
        return a.stage.localeCompare(b.stage);
    });

    printProfileResults(finalAveragedTimings);

    
    if (fs.existsSync(DB_PATH_STAX)) {
        fs.rmSync(DB_PATH_STAX, { recursive: true, force: true });
    }
}

function printProfileResults(timings) {
    const totalInsertTime = timings.find(t => t.stage === 'Insert: Total E2E').time_ms;
    const totalGetTime = timings.find(t => t.stage === 'Get: Total E2E').time_ms;
    
    console.log("\n" + "-".repeat(80));
    console.log(" " + "Stage".padEnd(28) + "| " + "Time (ms)".padEnd(15) + "| " + "% of Total".padEnd(15) + "| " + "Avg Latency (ns)".padEnd(20));
    console.log("-".repeat(80));

    for (const timing of timings) {
        const itemsToAvg = timing.stage.includes('E2E') ? NUM_ITEMS : ITEMS_PER_THREAD;
        const totalTime = timing.stage.startsWith('Insert') ? totalInsertTime : totalGetTime;
        const percentage = totalTime > 0 ? ((timing.time_ms / totalTime) * 100).toFixed(2) + '%' : 'N/A';
        const avgLatency = (timing.time_ms * 1000000 / itemsToAvg).toFixed(2);
        
        if (timing.isTotal) {
            console.log("-".repeat(80));
            console.log(" " + timing.stage.padEnd(28) + "| " + timing.time_ms.toFixed(3).padEnd(15) + "| " + "100.00%".padEnd(15) + "| " + avgLatency.padEnd(20));
        } else {
             console.log("   " + timing.stage.padEnd(25) + "| " + timing.time_ms.toFixed(3).padEnd(15) + "| " + percentage.padEnd(15) + "| " + avgLatency.padEnd(20));
        }
    }
    console.log("-".repeat(80));
}


if (isMainThread) {
    main().catch(err => console.error(err));
} else {
    workerLogic();
}