const fs = require('fs');
const path = require('path');
const assert = require('assert');
const { StaxDB, StaxResultSet, StaxGraph, StaxWriteTransaction, StaxGraphTransaction } = require('./index.js');

const DB_PATH = './staxdb_test_data';


async function runAllTests() {
    console.log('--- Starting StaxDB Node.js Addon Test Suite ---');
    
    
    if (fs.existsSync(DB_PATH)) {
        console.log(`Removing old test directory: ${DB_PATH}`);
        fs.rmSync(DB_PATH, { recursive: true, force: true });
    }

    let db;
    let testPassed = true;
    let finalMessage = 'All tests passed!';
    
    try {
        await testDropFunctionality();

        console.log('\nStep 1: Opening database...');
        db = new StaxDB(DB_PATH);
        console.log('Database opened successfully.');

        await testKVOperations(db);
        await testGraphScenarios(db);
        await testErrorHandling(db);
        await testGeoOperations(db);
        await testFluentQueryAPI(db);
        await testAdvancedFluentQueries(db);
        await testAsyncIterator(db);
        await testTransactions(db);
        await testDeletionIntegrity();
        await testApplicationDataIntegrity(db); 

    } catch (error) {
        console.error('\n--- A TEST SCENARIO FAILED ---');
        console.error(error);
        testPassed = false;
        finalMessage = error.message;
    } finally {
        console.log('\nFinal Step: Cleaning up resources...');
        if (db) {
            db.closeSync();
            console.log('  - Database closed.');
        }
        if (fs.existsSync(DB_PATH)) {
            fs.rmSync(DB_PATH, { recursive: true, force: true });
            console.log('  - Test directory removed.');
        }
    }

    console.log('\n-----------------------------------------');
    if (testPassed) {
        console.log(`✅ SUCCESS: ${finalMessage}`);
        process.exit(0);
    } else {
        console.log(`❌ FAILED: ${finalMessage}`);
        process.exit(1);
    }
}


function validateQuery(graph, resultSetsToClose, scenario) {
    console.log(`  - Running scenario: "${scenario.name}"`);
    const resultSet = scenario.builder ? scenario.builder.execute() : graph.executeQuery(scenario.plan);
    resultSetsToClose.push(resultSet);
    
    let actualResults = resultSet.getPage(1, resultSet.getTotalCount()).results;
    const count = actualResults.length;
    
    const expectedCount = scenario.expected ? scenario.expected.length : 0;

    assert.strictEqual(count, expectedCount, `FAIL [${scenario.name}]: Expected ${expectedCount} results, but got ${count}.`);

    if (count > 0) {
        const MAX_SAFE_INTEGER_STR_LEN = String(Number.MAX_SAFE_INTEGER).length;

        const processResults = (results) => {
            return results.map(obj => {
                const newObj = {};
                for (const key in obj) {
                    const value = obj[key];
                    if (key === '__stax_id') {
                        newObj[key] = Number(value);
                        continue;
                    }
                    if (typeof value === 'string' && /^\d+$/.test(value)) {
                         if (value.length >= MAX_SAFE_INTEGER_STR_LEN) {
                            newObj[key] = BigInt(value);
                        } else {
                            newObj[key] = Number(value);
                        }
                    } else {
                        newObj[key] = value;
                    }
                }
                return newObj;
            });
        };
        
        const processedActualResults = processResults(actualResults);
        const processedExpectedResults = processResults(scenario.expected);

        const sortKey = scenario.sortKey || '__stax_id';
        const sortedActual = processedActualResults.sort((a, b) => {
            const valA = a[sortKey];
            const valB = b[sortKey];
            if (typeof valA === 'bigint' && typeof valB === 'bigint') {
                return valA < valB ? -1 : valA > valB ? 1 : 0;
            }
            return String(valA).localeCompare(String(valB), undefined, {numeric: true});
        });
        const sortedExpected = processedExpectedResults.sort((a, b) => {
            const valA = a[sortKey];
            const valB = b[sortKey];
             if (typeof valA === 'bigint' && typeof valB === 'bigint') {
                return valA < valB ? -1 : valA > valB ? 1 : 0;
            }
            return String(valA).localeCompare(String(valB), undefined, {numeric: true});
        });
        
        assert.deepStrictEqual(sortedActual, sortedExpected, `FAIL [${scenario.name}]: Result object mismatch.`);
    }
    console.log(`    ... PASSED`);
}

async function testDropFunctionality() {
    console.log('\n--- SCENARIO: Drop Functionality ---');
    const DROP_PATH = './staxdb_drop_test';

    
    console.log('  - Creating a temporary database...');
    let tempDb = new StaxDB(DROP_PATH);
    tempDb.getCollectionSync('dummy').insertSync('key', 'value');
    tempDb.closeSync();
    assert.strictEqual(fs.existsSync(DROP_PATH), true, 'FAIL: DB directory was not created.');
    console.log('    ... PASSED');

    
    console.log('  - Dropping the database...');
    StaxDB.dropSync(DROP_PATH);
    console.log('    ... PASSED');

    
    console.log('  - Verifying directory is deleted...');
    assert.strictEqual(fs.existsSync(DROP_PATH), false, 'FAIL: DB directory was not removed by dropSync.');
    console.log('    ... PASSED');
    
    
    console.log('  - Dropping a non-existent path (should not throw)...');
    try {
        StaxDB.dropSync('./non_existent_dir_for_drop_test');
        console.log('    ... PASSED');
    } catch (e) {
        assert.fail(`FAIL: dropSync threw an error on a non-existent path: ${e.message}`);
    }
}


async function testKVOperations(db) {
    console.log('\n--- SCENARIO: Key-Value Operations ---');
    const users = db.getCollectionSync('users');
    let resultSetsToClose = [];

    try {
        
        console.log('  - Testing single-shot KV operations...');
        users.insertSync('user:1', 'Alice');
        const value = users.getSync('user:1').toString('utf8');
        assert.strictEqual(value, 'Alice', `KV Get validation failed. Expected "Alice", got "${value}".`);
        users.removeSync('user:1');
        const removedValue = users.getSync('user:1');
        assert.strictEqual(removedValue, undefined, `KV verification after remove failed.`);
        console.log('    ... PASSED');

        
        console.log('  - Testing edge case values (empty, large)...');
        users.insertSync('user:empty', '');
        assert.strictEqual(users.getSync('user:empty').toString('utf8'), '', 'Empty value not handled correctly.');
        const largeValue = 'a'.repeat(100 * 1024); 
        users.insertSync('user:large', largeValue);
        assert.strictEqual(users.getSync('user:large').toString('utf8'), largeValue, 'Large value not handled correctly.');
        console.log('    ... PASSED');

        
        console.log('  - Testing mixed batch operations...');
        const batchOps = [
            { type: 'insert', key: 'batch:1', value: 'A' },
            { type: 'insert', key: 'batch:2', value: 'B' },
            { type: 'insert', key: 'batch:3', value: 'C' },
            { type: 'remove', key: 'batch:2' }
        ];
        users.executeBatch(batchOps);
        assert.strictEqual(users.getSync('batch:1').toString('utf8'), 'A', 'Batch insert failed for batch:1');
        assert.strictEqual(users.getSync('batch:2'), undefined, 'Batch remove failed for batch:2');
        assert.strictEqual(users.getSync('batch:3').toString('utf8'), 'C', 'Batch insert failed for batch:3');
        console.log('    ... PASSED');

        
        console.log('  - Populating data for KV pagination test...');
        const pageTestData = [];
        for (let i = 0; i < 25; i++) {
            pageTestData.push({ type: 'insert', key: `page_user:${i.toString().padStart(2, '0')}`, value: `User number ${i}` });
        }
        users.executeBatch(pageTestData);
        console.log('    Inserted 25 records.');

        console.log('  - Testing KV range query and pagination...');
        const kvResultSet = users.range({ start: 'page_user:', end: 'page_user:~' });
        resultSetsToClose.push(kvResultSet);

        assert.strictEqual(kvResultSet.getTotalCount(), 25, 'KV getTotalCount validation failed.');
        
        const page1 = kvResultSet.getPage(1, 10);
        assert.strictEqual(page1.results.length, 10, 'KV Page 1 item count validation failed.');
        assert.strictEqual(page1.results[0].key.toString('utf8'), 'page_user:00', 'KV Page 1, Item 0 key mismatch.');
        assert.strictEqual(page1.results[9].key.toString('utf8'), 'page_user:09', 'KV Page 1, Item 9 key mismatch.');

        const page3 = kvResultSet.getPage(3, 10);
        assert.strictEqual(page3.results.length, 5, 'KV Page 3 item count validation failed.');
        assert.strictEqual(page3.results[4].key.toString('utf8'), 'page_user:24', 'KV Page 3, last item key mismatch.');
        
        const page4 = kvResultSet.getPage(4, 10);
        assert.strictEqual(page4.results.length, 0, 'Out-of-bounds page should be empty.');
        
        console.log('    ... PASSED');

    } finally {
        for(const rs of resultSetsToClose) {
            if(rs) rs.close();
        }
    }
}


async function testGraphScenarios(db) {
    console.log('\n--- SCENARIO: Graph Operations & Queries ---');
    const graph = db.getGraph();
    let resultSetsToClose = [];

    try {
        const user101_id = graph.insertObject({ type: 'user', name: 'User_101', role: 'dev', city: 'London', status: 'active', age: 25 });
        const user102_id = graph.insertObject({ type: 'user', name: 'User_102', role: 'dev', city: 'Paris', status: 'active', age: 35 });
        const user103_id = graph.insertObject({ type: 'user', name: 'User_103', role: 'dev', city: 'London', status: 'inactive', age: 45 });
        const user201_id = graph.insertObject({ type: 'user', name: 'User_201', role: 'manager', city: 'Paris', status: 'active', age: 40 });
        const user202_id = graph.insertObject({ type: 'user', name: 'User_202', role: 'manager', city: 'London', status: 'active', age: 50 });
        const user301_id = graph.insertObject({ type: 'user', name: 'User_301', role: 'designer', city: 'Tokyo', status: 'active', age: 30 });
        const user302_id = graph.insertObject({ type: 'user', name: 'User_302', role: 'designer', city: 'Paris', status: 'inactive', age: 28 });
        
        const project401_id = graph.insertObject({ type: 'project', name: 'StaxDB', status: 'active'});
        const project402_id = graph.insertObject({ type: 'project', name: 'GraphViz', status: 'active'});
        
        graph.insertRelationship(user101_id, 'WORKS_ON', project401_id);
        graph.insertRelationship(user102_id, 'WORKS_ON', project401_id);
        graph.insertRelationship(user201_id, 'MANAGES', project401_id);
        graph.insertRelationship(user202_id, 'MANAGES', project402_id);
        graph.insertRelationship(user301_id, 'WORKS_ON', project402_id);
        
        graph.insertRelationship(user101_id, 'REPORTS_TO', user201_id);
        graph.insertRelationship(user102_id, 'REPORTS_TO', user201_id);
        graph.insertRelationship(user301_id, 'REPORTS_TO', user202_id);

        graph.commit();
        
        console.log('\n  --- Sub-Scenario: Update and Delete ---');
        
        console.log('    - Step 1: Initial Insert');
        const widgetId = graph.insertObject({ name: 'Widget', price: 100, color: 'blue' });
        graph.commit();

        validateQuery(graph, resultSetsToClose, { name: "Verify initial widget price", plan: [{ op_type: 'find', field: 'price', value: { gte: 100, lte: 100 } }], expected: [{ __stax_id: widgetId, name: 'Widget', price: 100, color: 'blue' }]});
        validateQuery(graph, resultSetsToClose, { name: "Verify initial widget color", plan: [{ op_type: 'find', field: 'color', value: 'blue' }], expected: [{ __stax_id: widgetId, name: 'Widget', price: 100, color: 'blue' }] });

        console.log('    - Step 2: Update Object (Modify price, add status, remove color)');
        graph.updateObject(widgetId, { name: 'Widget', price: 150, status: 'updated_status' });
        graph.commit();
        validateQuery(graph, resultSetsToClose, { name: "Verify price is NOT 100 after update", plan: [{ op_type: 'find', field: 'price', value: { gte: 100, lte: 100 } }], expected: []});
        validateQuery(graph, resultSetsToClose, { name: "Verify price IS 150 after update", plan: [{ op_type: 'find', field: 'price', value: { gte: 150, lte: 150 } }], expected: [{ __stax_id: widgetId, name: 'Widget', price: 150, status: 'updated_status' }]});
        validateQuery(graph, resultSetsToClose, { name: "Verify color is GONE after update", plan: [{ op_type: 'find', field: 'color', value: 'blue' }], expected: []});
        validateQuery(graph, resultSetsToClose, { name: "Verify new status exists", plan: [{ op_type: 'find', field: 'status', value: 'updated_status' }], expected: [{ __stax_id: widgetId, name: 'Widget', price: 150, status: 'updated_status' }]});
        
        console.log('    - Step 3: Delete Object');
        graph.deleteObject(widgetId);
        graph.commit();
        validateQuery(graph, resultSetsToClose, { name: "Verify object is GONE by name", plan: [{ op_type: 'find', field: 'name', value: 'Widget' }], expected: []});
        validateQuery(graph, resultSetsToClose, { name: "Verify object is GONE by price", plan: [{ op_type: 'find', field: 'price', value: { gte: 150, lte: 150 } }], expected: []});
        validateQuery(graph, resultSetsToClose, { name: "Verify object is GONE by status", plan: [{ op_type: 'find', field: 'status', value: 'updated_status' }], expected: []});
        console.log('    ... Update/Delete PASSED');

    } finally {
        for(const rs of resultSetsToClose) {
            if(rs) rs.close();
        }
    }
}


async function testErrorHandling(db) {
    console.log('\n--- SCENARIO: Error Handling ---');
    const graph = db.getGraph();

    
    try {
        graph.executeQuery("this is not a plan");
        assert.fail("executeQuery should have thrown an error for an invalid plan.");
    } catch (e) {
        assert.ok(e.message.includes('requires a query plan'), 'Correct error for invalid plan type.');
        console.log('  - PASSED: Correctly threw error for invalid query plan.');
    }

    
    try {
        graph.executeQuery([{ op_type: 'find', field: 'name' }]); 
        assert.fail("executeQuery should have thrown an error for a malformed step.");
    } catch (e) {
        assert.ok(e, 'Correct error for malformed query step.');
        console.log('  - PASSED: Correctly threw error for malformed query step.');
    }

    
    const tempDb = new StaxDB(DB_PATH + '_temp');
    tempDb.closeSync();
    try {
        tempDb.getCollectionSync('some_collection');
        assert.fail("Using a closed DB should throw an error.");
    } catch (e) {
        assert.strictEqual(e.message, 'Database is not open.', 'Correct error for using closed DB.');
        console.log('  - PASSED: Correctly threw error for using a closed database.');
    }
    StaxDB.dropSync(DB_PATH + '_temp');
}

async function testGeoOperations(db) {
    console.log('\n--- SCENARIO: Geolocation Operations ---');
    const graph = db.getGraph();
    let resultSetsToClose = [];

    try {
        console.log('  - Populating geolocation data...');
        const googleplex_id = graph.insertObject({ name: 'googleplex', type: 'hq', location: { lat: 37.422, lon: -122.084 } });
        const applepark_id = graph.insertObject({ name: 'applepark', type: 'hq', location: { lat: 37.334, lon: -122.009 } });
        const sutro_tower_id = graph.insertObject({ name: 'sutro_tower', type: 'landmark', location: { lat: 37.755, lon: -122.452 } });

        graph.commit();
        console.log('    ... Geolocation data populated.');

        const geoScenarios = [
            {
                name: "Geo: Find Googleplex by exact coordinate",
                plan: [{ op_type: 'find', field: 'location', value: { lat: 37.422, lon: -122.084 } }],
                expected: [ { __stax_id: googleplex_id, name: 'googleplex', type: 'hq', location: 5589826863130677144n } ]
            },
        ];

        for (const scenario of geoScenarios) {
            validateQuery(graph, resultSetsToClose, scenario);
        }

    } finally {
        for(const rs of resultSetsToClose) {
            if(rs) rs.close();
        }
    }
}

async function testFluentQueryAPI(db) {
    console.log('\n--- SCENARIO: Fluent Query API ---');
    const graph = db.getGraph();
    let resultSetsToClose = [];

    
    const user_london_dev_results = graph.query().find({ city: 'London', role: 'dev' }).execute().getPage(1,10).results;
    const user_paris_manager_results = graph.query().find({ city: 'Paris', role: 'manager' }).execute().getPage(1,10).results;
    const user_london_inactive_results = graph.query().find({ status: 'inactive', city: 'London' }).execute().getPage(1,10).results;
    const user_tokyo_designer_results = graph.query().find({ city: 'Tokyo', role: 'designer' }).execute().getPage(1,10).results;
    const user_paris_inactive_results = graph.query().find({ status: 'inactive', city: 'Paris' }).execute().getPage(1,10).results;

    const user_paris_manager = user_paris_manager_results[0];
    const user_london_inactive = user_london_inactive_results[0];
    const user_tokyo_designer = user_tokyo_designer_results[0];
    const user_paris_inactive = user_paris_inactive_results[0];


    try {
        const scenarios = [
            {
                name: "Fluent API: Find London devs and who they report to",
                builder: graph.query()
                              .find({ city: 'London', role: 'dev' })
                              .traverse('out', 'REPORTS_TO'),
                expected: [
                    { __stax_id: user_paris_manager.__stax_id, type: 'user', name: 'User_201', role: 'manager', city: 'Paris', status: 'active', age: 40 },
                ],
                sortKey: '__stax_id'
            },
            {
                name: "Fluent API: Find active managers, then filter to Paris",
                builder: graph.query()
                              .find({ role: 'manager', status: 'active' })
                              .filter({ city: 'Paris' }),
                expected: [
                    { __stax_id: user_paris_manager.__stax_id, type: 'user', name: 'User_201', role: 'manager', city: 'Paris', status: 'active', age: 40 },
                ],
                sortKey: '__stax_id'
            },
            {
                name: "Fluent API: Find designers in Tokyo OR inactive users",
                builder: graph.query()
                              .find([
                                  { role: 'designer', city: 'Tokyo' },
                                  { status: 'inactive' }
                              ]),
                expected: [
                    { __stax_id: user_london_inactive.__stax_id, type: 'user', name: 'User_103', role: 'dev', city: 'London', status: 'inactive', age: 45 },
                    { __stax_id: user_tokyo_designer.__stax_id, type: 'user', name: 'User_301', role: 'designer', city: 'Tokyo', status: 'active', age: 30 },
                    { __stax_id: user_paris_inactive.__stax_id, type: 'user', name: 'User_302', role: 'designer', city: 'Paris', status: 'inactive', age: 28 },
                ],
                sortKey: '__stax_id'
            },
            {
                name: "Fluent API: Traverse with Filter (London devs reporting to London manager)",
                builder: graph.query()
                              .find({ role: 'dev', city: 'London'})
                              .traverse('out', 'REPORTS_TO', { city: 'Paris' }),
                expected: [
                    { __stax_id: user_paris_manager.__stax_id, type: 'user', name: 'User_201', role: 'manager', city: 'Paris', status: 'active', age: 40 },
                ],
                sortKey: '__stax_id'
            }
        ];

        for (const scenario of scenarios) {
            validateQuery(graph, resultSetsToClose, scenario);
        }
    } finally {
        for(const rs of resultSetsToClose) {
            if(rs) rs.close();
        }
    }
}

async function testAdvancedFluentQueries(db) {
    console.log('\n--- SCENARIO: Advanced Fluent Queries (Ranges & Geo) ---');
    const graph = db.getGraph();
    let resultSetsToClose = [];
    
    const user_paris_dev_results = graph.query().find({ name: 'User_102' }).execute().getPage(1,1).results;
    const user_paris_manager_results = graph.query().find({ name: 'User_201' }).execute().getPage(1,1).results;
    const user_tokyo_designer_results = graph.query().find({ name: 'User_301' }).execute().getPage(1,1).results;
    const googleplex_results = graph.query().find({ name: 'googleplex' }).execute().getPage(1,1).results;
    const applepark_results = graph.query().find({ name: 'applepark' }).execute().getPage(1,1).results;

    const user_paris_dev = user_paris_dev_results[0];
    const user_paris_manager = user_paris_manager_results[0];
    const user_tokyo_designer = user_tokyo_designer_results[0];
    const googleplex = googleplex_results[0];
    const applepark = applepark_results[0];


    try {
        const scenarios = [
            {
                name: "Advanced Fluent: Find users with age between 30 and 40",
                builder: graph.query()
                              .find({ type: 'user', age: { gte: 30, lte: 40 } }),
                expected: [
                    { __stax_id: user_paris_dev.__stax_id, type: 'user', name: 'User_102', role: 'dev', city: 'Paris', status: 'active', age: 35 },
                    { __stax_id: user_paris_manager.__stax_id, type: 'user', name: 'User_201', role: 'manager', city: 'Paris', status: 'active', age: 40 },
                    { __stax_id: user_tokyo_designer.__stax_id, type: 'user', name: 'User_301', role: 'designer', city: 'Tokyo', status: 'active', age: 30 },
                ],
                sortKey: '__stax_id'
            },
            {
                name: "Advanced Fluent: Find HQs near Googleplex",
                builder: graph.query()
                              .find({ type: 'hq', location: { near: { lat: 37.4, lon: -122.0 }, radius: 10000 } }),
                expected: [
                     { __stax_id: googleplex.__stax_id, name: 'googleplex', type: 'hq', location: 5589826863130677144n },
                     { __stax_id: applepark.__stax_id, name: 'applepark', type: 'hq', location: 5589824623460035215n },
                ],
                sortKey: '__stax_id'
            }
        ];

        for (const scenario of scenarios) {
            validateQuery(graph, resultSetsToClose, scenario);
        }
    } finally {
        for(const rs of resultSetsToClose) {
            if(rs) rs.close();
        }
    }
}

async function testAsyncIterator(db) {
    console.log('\n--- SCENARIO: Async Iterator (`for await...of`) ---');
    const collection = db.getCollectionSync('iterator_test');
    
    console.log('  - Populating data for async iterator test...');
    const expectedData = [];
    const batchOps = [];
    for (let i = 0; i < 75; i++) {
        const key = `iter_key:${i.toString().padStart(3, '0')}`;
        const value = `Value for item ${i}`;
        batchOps.push({ type: 'insert', key, value });
        expectedData.push({ key, value });
    }
    collection.executeBatch(batchOps);
    console.log(`    Inserted ${expectedData.length} records.`);

    console.log('  - Testing iteration with `for await...of`...');
    const resultSet = collection.range({ start: 'iter_key:', end: 'iter_key:~' });
    const iteratedResults = [];
    
    for await (const record of resultSet) {
        iteratedResults.push({
            key: record.key.toString('utf8'),
            value: record.value.toString('utf8'),
        });
    }

    assert.strictEqual(iteratedResults.length, expectedData.length, 'Async iterator did not yield the correct number of items.');
    assert.deepStrictEqual(iteratedResults, expectedData, 'The items yielded by the async iterator do not match the expected data.');

    console.log(`    ... PASSED`);
}

async function testTransactions(db) {
    console.log('\n--- SCENARIO: Atomic Transactions ---');
    
    
    console.log('  --- Sub-Scenario: Key-Value Transactions ---');
    const accounts = db.getCollectionSync('accounts');
    accounts.insertSync('acct:1', '1000');
    accounts.insertSync('acct:2', '500');

    console.log('    - Testing successful commit...');
    const commitTxn = accounts.beginTransaction(false);
    commitTxn.insert('acct:1', '900');
    commitTxn.insert('acct:2', '600');
    commitTxn.commit();
    
    assert.strictEqual(accounts.getSync('acct:1').toString('utf8'), '900', 'KV Commit failed for acct:1');
    assert.strictEqual(accounts.getSync('acct:2').toString('utf8'), '600', 'KV Commit failed for acct:2');
    console.log('      ... PASSED');

    console.log('    - Testing abort (rollback)...');
    const abortTxn = accounts.beginTransaction(false);
    abortTxn.insert('acct:1', '0');
    abortTxn.remove('acct:2');
    abortTxn.abort();

    assert.strictEqual(accounts.getSync('acct:1').toString('utf8'), '900', 'KV Abort failed for acct:1');
    assert.strictEqual(accounts.getSync('acct:2').toString('utf8'), '600', 'KV Abort failed for acct:2');
    console.log('      ... PASSED');

    
    console.log('  --- Sub-Scenario: Graph Transactions ---');
    const graph = db.getGraph();
    const originalManagerId = graph.insertObject({ name: 'Original Manager' });
    graph.commit();

    console.log('    - Testing successful graph commit...');
    const gCommitTxn = graph.beginTransaction();
    const empId = gCommitTxn.insertObject({ name: 'New Employee' });
    const projId = gCommitTxn.insertObject({ name: 'New Project' });
    gCommitTxn.insertRelationship(empId, 'WORKS_ON', projId);
    gCommitTxn.insertRelationship(empId, 'REPORTS_TO', originalManagerId);
    gCommitTxn.commit();

    let results = graph.query().find({name: 'New Employee'}).traverse('out', 'WORKS_ON').execute().getPage(1, 10).results;
    assert.strictEqual(results.length, 1, 'Graph commit failed: WORKS_ON relationship not found.');
    assert.strictEqual(results[0].name, 'New Project', 'Graph commit failed: Incorrect project found.');
    console.log('      ... PASSED');

    console.log('    - Testing graph abort (rollback)...');
    const gAbortTxn = graph.beginTransaction();
    const tempEmpId = gAbortTxn.insertObject({ name: 'Temporary Employee' });
    gAbortTxn.insertRelationship(tempEmpId, 'REPORTS_TO', originalManagerId);
    gAbortTxn.abort();

    results = graph.query().find({name: 'Temporary Employee'}).execute().getPage(1,10).results;
    assert.strictEqual(results.length, 0, 'Graph abort failed: Temporary Employee should not exist.');
    console.log('      ... PASSED');
    
    console.log('    - Testing error handling on used transaction...');
    try {
        commitTxn.insert('acct:1', '1');
        assert.fail("Should have thrown error on used KV transaction.");
    } catch (e) {
        assert.ok(e.message.includes('committed or aborted'), 'Correct error for used KV transaction.');
        console.log('      ... PASSED');
    }
     try {
        gCommitTxn.insertObject({name: 'another'});
        assert.fail("Should have thrown error on used Graph transaction.");
    } catch (e) {
        assert.ok(e.message.includes('committed or aborted'), 'Correct error for used Graph transaction.');
        console.log('      ... PASSED');
    }
}

async function testDeletionIntegrity() {
    console.log('\n--- SCENARIO: Deletion Integrity ---');
    const DELETION_DB_PATH = './staxdb_deletion_test';
    if (fs.existsSync(DELETION_DB_PATH)) {
        StaxDB.dropSync(DELETION_DB_PATH);
    }

    let db = new StaxDB(DELETION_DB_PATH);
    let graph = db.getGraph();
    let resultSetsToClose = [];

    try {
        console.log('  - Step 1: Insert 3 items.');
        const item1_id = graph.insertObject({ type: 'item', name: 'Item A', value: 10 });
        const item2_id = graph.insertObject({ type: 'item', name: 'Item B', value: 20 });
        const item3_id = graph.insertObject({ type: 'item', name: 'Item C', value: 30 });
        graph.commit();

        validateQuery(graph, resultSetsToClose, { name: "Verify 3 items exist", plan: [{ op_type: 'find', field: 'type', value: 'item' }], expected: [
            { __stax_id: item1_id, type: 'item', name: 'Item A', value: 10 },
            { __stax_id: item2_id, type: 'item', name: 'Item B', value: 20 },
            { __stax_id: item3_id, type: 'item', name: 'Item C', value: 30 },
        ]});

        console.log('  - Step 2: Delete 1 item (Item B).');
        graph.deleteObject(item2_id);
        graph.commit();
        
        validateQuery(graph, resultSetsToClose, { name: "Verify Item B is gone", plan: [{ op_type: 'find', field: 'name', value: 'Item B' }], expected: []});
        
        console.log('  - Step 3: Verify the other 2 items still exist.');
        validateQuery(graph, resultSetsToClose, { name: "Verify 2 items remain", plan: [{ op_type: 'find', field: 'type', value: 'item' }], expected: [
            { __stax_id: item1_id, type: 'item', name: 'Item A', value: 10 },
            { __stax_id: item3_id, type: 'item', name: 'Item C', value: 30 },
        ]});

        console.log('  - Step 4: Close and reopen the database.');
        db.closeSync();
        db = new StaxDB(DELETION_DB_PATH);
        graph = db.getGraph();
        console.log('    ... Database reopened.');

        console.log('  - Step 5: Verify again after reopen.');
        validateQuery(graph, resultSetsToClose, { name: "Verify 2 items remain after reopen", plan: [{ op_type: 'find', field: 'type', value: 'item' }], expected: [
            { __stax_id: item1_id, type: 'item', name: 'Item A', value: 10 },
            { __stax_id: item3_id, type: 'item', name: 'Item C', value: 30 },
        ]});
        console.log('    ... Deletion Integrity PASSED');

    } finally {
        for(const rs of resultSetsToClose) { if(rs) rs.close(); }
        if (db) db.closeSync();
        StaxDB.dropSync(DELETION_DB_PATH);
    }
}

async function testApplicationDataIntegrity(db) {
    console.log('\n--- SCENARIO: Application Data Integrity (Dynamic Multi-Delete) ---');
    let graph = db.getGraph();
    let resultSetsToClose = [];

    try {
        console.log('  - Step 1: Insert 10 items with application schema.');
        let items = [];
        for (let i = 1; i <= 10; i++) {
            items.push({ 
                itemId: `item-${i}`, 
                type: "item", 
                title: `Item Title ${i}`, 
                price: 100 + i 
            });
        }
        
        const insertedIds = items.map(item => graph.insertObject(item));
        graph.commit();

        let expectedState = items.map((item, i) => ({ ...item, __stax_id: insertedIds[i] }));
        
        validateQuery(graph, resultSetsToClose, { 
            name: "Verify 10 app items exist", 
            plan: [{ op_type: 'find', field: 'type', value: 'item' }], 
            expected: expectedState,
            sortKey: 'price'
        });

        console.log('  - Step 2: Delete a middle item (item-5).');
        const item5_internal_id = expectedState.find(i => i.itemId === 'item-5').__stax_id;
        graph.deleteObject(item5_internal_id);
        graph.commit();

        expectedState = expectedState.filter(i => i.itemId !== 'item-5');
        validateQuery(graph, resultSetsToClose, { 
            name: "Verify 9 items remain after deleting item-5", 
            plan: [{ op_type: 'find', field: 'type', value: 'item' }], 
            expected: expectedState,
            sortKey: 'price'
        });
        
        console.log('  - Step 3: Delete another middle item (item-8).');
        const item8_internal_id = expectedState.find(i => i.itemId === 'item-8').__stax_id;
        graph.deleteObject(item8_internal_id);
        graph.commit();
        
        expectedState = expectedState.filter(i => i.itemId !== 'item-8');
        validateQuery(graph, resultSetsToClose, { 
            name: "Verify 8 items remain after deleting item-8", 
            plan: [{ op_type: 'find', field: 'type', value: 'item' }], 
            expected: expectedState,
            sortKey: 'price'
        });

        console.log('  - Step 4: CRITICAL TEST - Delete the very first item (item-1).');
        const item1_internal_id = expectedState.find(i => i.itemId === 'item-1').__stax_id;
        graph.deleteObject(item1_internal_id);
        graph.commit();

        expectedState = expectedState.filter(i => i.itemId !== 'item-1');
        assert.strictEqual(expectedState.length, 7, "Final expected state should have 7 items.");
        
        validateQuery(graph, resultSetsToClose, { 
            name: "Verify 7 items remain after deleting item-1", 
            plan: [{ op_type: 'find', field: 'type', value: 'item' }], 
            expected: expectedState,
            sortKey: 'price'
        });
        
        console.log('  - Step 5: Delete the last remaining item (item-10).');
        const item10_internal_id = expectedState.find(i => i.itemId === 'item-10').__stax_id;
        graph.deleteObject(item10_internal_id);
        graph.commit();

        expectedState = expectedState.filter(i => i.itemId !== 'item-10');
        assert.strictEqual(expectedState.length, 6, "Final expected state should have 6 items.");

        validateQuery(graph, resultSetsToClose, { 
            name: "Verify 6 items remain after deleting item-10", 
            plan: [{ op_type: 'find', field: 'type', value: 'item' }], 
            expected: expectedState,
            sortKey: 'price'
        });

        console.log('  - Step 6: Delete the new first item (item-2).');
        const item2_internal_id = expectedState.find(i => i.itemId === 'item-2').__stax_id;
        graph.deleteObject(item2_internal_id);
        graph.commit();

        expectedState = expectedState.filter(i => i.itemId !== 'item-2');
        assert.strictEqual(expectedState.length, 5, "Final expected state should have 5 items.");

        validateQuery(graph, resultSetsToClose, { 
            name: "Verify 5 items remain after deleting item-2", 
            plan: [{ op_type: 'find', field: 'type', value: 'item' }], 
            expected: expectedState,
            sortKey: 'price'
        });


        console.log('    ... Application Data Integrity PASSED');

    } finally {
        for(const rs of resultSetsToClose) { if(rs) rs.close(); }
    }
}


runAllTests();