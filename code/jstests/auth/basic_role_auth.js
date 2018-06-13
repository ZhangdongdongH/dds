/**
 * This file tests basic authorization for different roles in stand alone and sharded
 * environment. This file covers all types of operations except commands.
 */

/**
 * Data structure that contains all the users that are going to be used in the tests.
 * The structure is as follows:
 * 1st level field name: database names.
 * 2nd level field name: user names.
 * 3rd level is an object that has the format:
 *     { pwd: <password>, roles: [<list of roles>] }
 */
var AUTH_INFO = {
    admin: {
        admin: {pwd: 'Github@12Root', roles: ['root']},
        monitor: {pwd: 'Github@12Root', roles: ['root']},
        cluster: {pwd: 'Github@12cluster', roles: ['clusterAdmin']},
        anone: {pwd: 'Github@12none', roles: []},
        aro: {pwd: 'Github@12ro', roles: ['read']},
        backupuser: {pwd: 'Github@12rw', roles: ['readWrite']},
        aadmin: {pwd: 'Github@12admin', roles: ['dbAdmin']},
        auadmin: {pwd: 'Github@12uadmin', roles: ['userAdmin']},
        any_ro: {pwd: 'Github@12ro', roles: ['readAnyDatabase']},
        any_rw: {pwd: 'Github@12rw', roles: ['readWriteAnyDatabase']},
        any_admin: {pwd: 'Github@12admin', roles: ['dbAdminAnyDatabase']},
        any_uadmin: {pwd: 'Github@12uadmin', roles: ['userAdminAnyDatabase']}
    },
    test: {
        none: {pwd: 'Github@12none', roles: []},
        ro: {pwd: 'Github@12ro', roles: ['read']},
        rw: {pwd: 'Github@12rw', roles: ['readWrite']},
        roadmin: {pwd: 'Github@12roadmin', roles: ['read', 'dbAdmin']},
        admin: {pwd: 'Github@12admin', roles: ['dbAdmin']},
        uadmin: {pwd: 'Github@12uadmin', roles: ['userAdmin']}
    }
};

// Constants that lists the privileges of a given role.
var READ_PERM = {query: 1, index_r: 1, killCursor: 1};
var READ_WRITE_PERM =
    {insert: 1, update: 1, remove: 1, query: 1, index_r: 1, index_w: 1, killCursor: 1};
var ADMIN_PERM = {index_r: 1, index_w: 1, profile_r: 1};
var UADMIN_PERM = {user_r: 1, user_w: 1};
var CLUSTER_PERM = {killOp: 1, currentOp: 1, fsync_unlock: 1, killCursor: 1, profile_r: 1};

/**
 * Checks whether an error occurs after running an operation.
 *
 * @param shouldPass {Boolean} true means that the operation should succeed.
 * @param opFunc {function()} a function object which contains the operation to perform.
 */
var checkErr = function(shouldPass, opFunc) {
    var success = true;

    var exception = null;
    try {
        opFunc();
    } catch (x) {
        exception = x;
        success = false;
    }

    assert(success == shouldPass,
           'expected shouldPass: ' + shouldPass + ', got: ' + success + ', op: ' + tojson(opFunc) +
               ', exception: ' + tojson(exception));
};

/**
 * Runs a series of operations against the db provided.
 *
 * @param db {DB} the database object to use.
 * @param allowedActions {Object} the lists of operations that are allowed for the
 *     current user. The data structure is represented as a map with the presence of
 *     a field name means that the operation is allowed and not allowed if it is
 *     not present. The list of field names are: insert, update, remove, query, killOp,
 *     currentOp, index_r, index_w, profile_r, profile_w, user_r, user_w, killCursor,
 *     fsync_unlock.
 */
var testOps = function(db, allowedActions) {
    checkErr(allowedActions.hasOwnProperty('insert'), function() {
        var res = db.user.insert({y: 1});
        if (res.hasWriteError())
            throw Error("insert failed: " + tojson(res.getRawResponse()));
    });

    checkErr(allowedActions.hasOwnProperty('update'), function() {
        var res = db.user.update({y: 1}, {z: 3});
        if (res.hasWriteError())
            throw Error("update failed: " + tojson(res.getRawResponse()));
    });

    checkErr(allowedActions.hasOwnProperty('remove'), function() {
        var res = db.user.remove({y: 1});
        if (res.hasWriteError())
            throw Error("remove failed: " + tojson(res.getRawResponse()));
    });

    checkErr(allowedActions.hasOwnProperty('query'), function() {
        db.user.findOne({y: 1});
    });

    checkErr(allowedActions.hasOwnProperty('killOp'), function() {
        var errorCodeUnauthorized = 13;
        var res = db.killOp(1);

        if (res.code == errorCodeUnauthorized) {
            throw Error("unauthorized killOp");
        }
    });

    checkErr(allowedActions.hasOwnProperty('currentOp'), function() {
        var errorCodeUnauthorized = 13;
        var res = db.currentOp();

        if (res.code == errorCodeUnauthorized) {
            throw Error("unauthorized currentOp");
        }
    });

    checkErr(allowedActions.hasOwnProperty('index_r'), function() {
        db.system.indexes.findOne();
    });

    checkErr(allowedActions.hasOwnProperty('index_w'), function() {
        var res = db.user.ensureIndex({x: 1});
        if (res.code == 13) {  // Unauthorized
            throw Error("unauthorized currentOp");
        }
    });

    checkErr(allowedActions.hasOwnProperty('profile_r'), function() {
        db.system.profile.findOne();
    });

    checkErr(allowedActions.hasOwnProperty('profile_w'), function() {
        var res = db.system.profile.insert({x: 1});
        if (res.hasWriteError()) {
            throw Error("profile insert failed: " + tojson(res.getRawResponse()));
        }
    });

    checkErr(allowedActions.hasOwnProperty('user_r'), function() {
        var result = db.runCommand({usersInfo: 1});
        if (!result.ok) {
            throw new Error(tojson(result));
        }
    });

    checkErr(allowedActions.hasOwnProperty('user_w'), function() {
        db.createUser({user: 'a', pwd: 'Github@12', roles: jsTest.basicUserRoles, "passwordDigestor" : "server"});
        assert(db.dropUser('a'));
    });

    // Test for kill cursor
    (function() {
        var newConn = new Mongo(db.getMongo().host);
        var dbName = db.getName();
        var db2 = newConn.getDB(dbName);

        if (db2 == 'admin') {
            assert.eq(1, db2.auth('aro', AUTH_INFO.admin.aro.pwd));
        } else {
            assert.eq(1, db2.auth('ro', AUTH_INFO.test.ro.pwd));
        }

        var cursor = db2.kill_cursor.find().batchSize(2);

        db.killCursor(cursor.id());
        // Send a synchronous message to make sure that kill cursor was processed
        // before proceeding.
        db.runCommand({whatsmyuri: 1});

        checkErr(!allowedActions.hasOwnProperty('killCursor'), function() {
            while (cursor.hasNext()) {
                var next = cursor.next();

                // This is a failure in mongos case. Standalone case will fail
                // when next() was called.
                if (next.code == 16336) {
                    // could not find cursor in cache for id
                    throw next.$err;
                }
            }
        });
    });  // TODO: enable test after SERVER-5813 is fixed.

    var isMongos = db.runCommand({isdbgrid: 1}).isdbgrid;
    // Note: fsyncUnlock is not supported in mongos.
    if (!isMongos) {
        checkErr(allowedActions.hasOwnProperty('fsync_unlock'), function() {
            var res = db.fsyncUnlock();
            var errorCodeUnauthorized = 13;

            if (res.code == errorCodeUnauthorized) {
                throw Error("unauthorized unauthorized fsyncUnlock");
            }
        });
    }
};

// List of tests to run. Has the format:
//
// {
//   name: {String} description of the test
//   test: {function(Mongo)} the test function to run which accepts a Mongo connection
//                           object.
// }
var TESTS = [
    {
      name: 'Test multiple user login separate connection',
      test: function(conn) {
          var testDB = conn.getDB('test');
          assert.eq(1, testDB.auth('ro', AUTH_INFO.test.ro.pwd));

          var conn2 = new Mongo(conn.host);
          var testDB2 = conn2.getDB('test');
          assert.eq(1, testDB2.auth('uadmin', AUTH_INFO.test.uadmin.pwd));

          testOps(testDB, READ_PERM);
          testOps(testDB2, UADMIN_PERM);
      }
    },
    {
      name: 'Test user with no role',
      test: function(conn) {
          var testDB = conn.getDB('test');
          assert.eq(1, testDB.auth('none', AUTH_INFO.test.none.pwd));

          testOps(testDB, {});
      }
    },
    {
      name: 'Test read only user',
      test: function(conn) {
          var testDB = conn.getDB('test');
          assert.eq(1, testDB.auth('ro', AUTH_INFO.test.ro.pwd));

          testOps(testDB, READ_PERM);
      }
    },
    {
      name: 'Test read/write user',
      test: function(conn) {
          var testDB = conn.getDB('test');
          assert.eq(1, testDB.auth('rw', AUTH_INFO.test.rw.pwd));

          testOps(testDB, READ_WRITE_PERM);
      }
    },
    {
      name: 'Test read + dbAdmin user',
      test: function(conn) {
          var testDB = conn.getDB('test');
          assert.eq(1, testDB.auth('roadmin', AUTH_INFO.test.roadmin.pwd));

          var combinedPerm = Object.extend({}, READ_PERM);
          combinedPerm = Object.extend(combinedPerm, ADMIN_PERM);
          testOps(testDB, combinedPerm);
      }
    },
    {
      name: 'Test dbAdmin user',
      test: function(conn) {
          var testDB = conn.getDB('test');
          assert.eq(1, testDB.auth('admin', AUTH_INFO.test.admin.pwd));

          testOps(testDB, ADMIN_PERM);
      }
    },
    {
      name: 'Test userAdmin user',
      test: function(conn) {
          var testDB = conn.getDB('test');
          assert.eq(1, testDB.auth('uadmin', AUTH_INFO.test.uadmin.pwd));

          testOps(testDB, UADMIN_PERM);
      }
    },
    {
      name: 'Test cluster user',
      test: function(conn) {
          var adminDB = conn.getDB('admin');
          print("eharry: Test cluster user1");
          
          var newDB = new Mongo(conn.host).getDB('admin');
          print("eharry: Test cluster user2");
          assert.eq(1, newDB.auth('admin', AUTH_INFO.admin.admin.pwd));
          print("eharry: Test cluster user3");
          res = newDB.updateUser('monitor', {roles: AUTH_INFO.admin.cluster.roles});
          print("eharry res is " + tojson(res));
          print("eharry: Test cluster user4");
          
          assert.eq(1, adminDB.auth('monitor', AUTH_INFO.admin.monitor.pwd));

          print("eharry: Test cluster user")

          testOps(conn.getDB('test'), CLUSTER_PERM);
      }
    },
    {
      name: 'Test admin user with no role',
      test: function(conn) {
          var adminDB = conn.getDB('admin');
          var newConn = new Mongo(conn.host);
          assert.eq(1, newConn.getDB('admin').auth('admin', AUTH_INFO.admin.admin.pwd));
          newConn.getDB('admin').updateUser('monitor', {roles: AUTH_INFO.admin.anone.roles});
          
          assert.eq(1, adminDB.auth('monitor', AUTH_INFO.admin.monitor.pwd));

          testOps(adminDB, {});
          testOps(conn.getDB('test'), {});
      }
    },
    {
      name: 'Test read only admin user',
      test: function(conn) {
          var adminDB = conn.getDB('admin');
          var newConn = new Mongo(conn.host);
          assert.eq(1, newConn.getDB('admin').auth('admin', AUTH_INFO.admin.admin.pwd));
          newConn.getDB('admin').updateUser('monitor', {roles: AUTH_INFO.admin.aro.roles});
          
          assert.eq(1, adminDB.auth('monitor', AUTH_INFO.admin.monitor.pwd));

          testOps(adminDB, READ_PERM);
          testOps(conn.getDB('test'), {});
      }
    },
    {
      name: 'Test read/write admin user',
      test: function(conn) {
          var adminDB = conn.getDB('admin');
          var newConn = new Mongo(conn.host);
          assert.eq(1, newConn.getDB('admin').auth('admin', AUTH_INFO.admin.admin.pwd));
          newConn.getDB('admin').updateUser('monitor', {roles: AUTH_INFO.admin.backupuser.roles});
          
          assert.eq(1, adminDB.auth('monitor', AUTH_INFO.admin.monitor.pwd));

          testOps(adminDB, READ_WRITE_PERM);
          testOps(conn.getDB('test'), {});
      }
    },
    {
      name: 'Test dbAdmin admin user',
      test: function(conn) {
          var adminDB = conn.getDB('admin');
          var newConn = new Mongo(conn.host);
          assert.eq(1, newConn.getDB('admin').auth('admin', AUTH_INFO.admin.admin.pwd));
          newConn.getDB('admin').updateUser('monitor', {roles: AUTH_INFO.admin.aadmin.roles});
          
          assert.eq(1, adminDB.auth('monitor', AUTH_INFO.admin.monitor.pwd));

          testOps(adminDB, ADMIN_PERM);
          testOps(conn.getDB('test'), {});
      }
    },
    {
      name: 'Test userAdmin admin user',
      test: function(conn) {
          var adminDB = conn.getDB('admin');
          var newConn = new Mongo(conn.host);
          assert.eq(1, newConn.getDB('admin').auth('admin', AUTH_INFO.admin.admin.pwd));
          newConn.getDB('admin').updateUser('monitor', {roles: AUTH_INFO.admin.auadmin.roles});
          
          assert.eq(1, adminDB.auth('monitor', AUTH_INFO.admin.monitor.pwd));

          testOps(adminDB, UADMIN_PERM);
          testOps(conn.getDB('test'), {});
      }
    },
    {
      name: 'Test read only any db user',
      test: function(conn) {
          var adminDB = conn.getDB('admin');
          var newConn = new Mongo(conn.host);
          assert.eq(1, newConn.getDB('admin').auth('admin', AUTH_INFO.admin.admin.pwd));
          newConn.getDB('admin').updateUser('monitor', {roles: AUTH_INFO.admin.any_ro.roles});
          
          assert.eq(1, adminDB.auth('monitor', AUTH_INFO.admin.monitor.pwd));

          testOps(adminDB, READ_PERM);
          testOps(conn.getDB('test'), READ_PERM);
      }
    },
    {
      name: 'Test read/write any db user',
      test: function(conn) {
          var adminDB = conn.getDB('admin');
          var newConn = new Mongo(conn.host);
          assert.eq(1, newConn.getDB('admin').auth('admin', AUTH_INFO.admin.admin.pwd));
          newConn.getDB('admin').updateUser('monitor', {roles: AUTH_INFO.admin.any_rw.roles});
          
          assert.eq(1, adminDB.auth('monitor', AUTH_INFO.admin.monitor.pwd));

          testOps(adminDB, READ_WRITE_PERM);
          testOps(conn.getDB('test'), READ_WRITE_PERM);
      }
    },
    {
      name: 'Test dbAdmin any db user',
      test: function(conn) {
          var adminDB = conn.getDB('admin');
          var newConn = new Mongo(conn.host);
          assert.eq(1, newConn.getDB('admin').auth('admin', AUTH_INFO.admin.admin.pwd));
          newConn.getDB('admin').updateUser('monitor', {roles: AUTH_INFO.admin.any_admin.roles});
          
          assert.eq(1, adminDB.auth('monitor', AUTH_INFO.admin.monitor.pwd));

          testOps(adminDB, ADMIN_PERM);
          testOps(conn.getDB('test'), ADMIN_PERM);
      }
    },
    {
      name: 'Test userAdmin any db user',
      test: function(conn) {
          var adminDB = conn.getDB('admin');
          var newConn = new Mongo(conn.host);
          assert.eq(1, newConn.getDB('admin').auth('admin', AUTH_INFO.admin.admin.pwd));
          newConn.getDB('admin').updateUser('monitor', {roles: AUTH_INFO.admin.any_uadmin.roles});
          
          assert.eq(1, adminDB.auth('monitor', AUTH_INFO.admin.monitor.pwd));

          testOps(adminDB, UADMIN_PERM);
          testOps(conn.getDB('test'), UADMIN_PERM);
      }
    },

    {
      name: 'Test change role',
      test: function(conn) {
          var testDB = conn.getDB('test');
          assert.eq(1, testDB.auth('rw', AUTH_INFO.test.rw.pwd));

          var newConn = new Mongo(conn.host);
          assert.eq(1, newConn.getDB('admin').auth('any_uadmin', AUTH_INFO.admin.any_uadmin.pwd));
          newConn.getDB('test').updateUser('rw', {roles: ['read']});
          var origSpec = newConn.getDB("test").getUser("rw");

          // role change should affect users already authenticated.
          testOps(testDB, READ_PERM);

          // role change should affect active connections.
          testDB.runCommand({logout: 1});
          assert.eq(1, testDB.auth('rw', AUTH_INFO.test.rw.pwd));
          testOps(testDB, READ_PERM);

          // role change should also affect new connections.
          var newConn3 = new Mongo(conn.host);
          var testDB3 = newConn3.getDB('test');
          assert.eq(1, testDB3.auth('rw', AUTH_INFO.test.rw.pwd));
          testOps(testDB3, READ_PERM);

          newConn.getDB('test').updateUser('rw', {roles: origSpec.roles});
      }
    },

    {
      name: 'Test override user',
      test: function(conn) {
          var testDB = conn.getDB('test');
          assert.eq(1, testDB.auth('rw', AUTH_INFO.test.rw.pwd));
          assert.eq(1, testDB.auth('ro', AUTH_INFO.test.ro.pwd));
          testOps(testDB, READ_PERM);

          testDB.runCommand({logout: 1});
          testOps(testDB, {});
      }
    }
];

/**
 * Driver method for setting up the test environment, running them, cleanup
 * after every test and keeping track of test failures.
 *
 * @param conn {Mongo} a connection to a mongod or mongos to test.
 */
var runTests = function(conn) {
    var setup = function() {
        var testDB = conn.getDB('test');
        var adminDB = conn.getDB('admin');

        adminDB.createUser(
            {user: 'admin', pwd: AUTH_INFO.admin.admin.pwd, roles: AUTH_INFO.admin.admin.roles, "passwordDigestor" : "server"});
        adminDB.auth('admin', AUTH_INFO.admin.admin.pwd);

        for (var x = 0; x < 10; x++) {
            testDB.kill_cursor.insert({x: x});
            adminDB.kill_cursor.insert({x: x});
        }

        for (var dbName in AUTH_INFO) {
            var dbObj = AUTH_INFO[dbName];

            for (var userName in dbObj) {
                if (dbName == 'admin' && userName == 'admin') {
                    // We already registered this user.
                    continue;
                }

                var info = dbObj[userName];
                conn.getDB(dbName).createUser({user: userName, pwd: info.pwd, roles: info.roles, "passwordDigestor" : "server"});
            }
        }

        adminDB.runCommand({logout: 1});
    };

    var teardown = function() {
        var adminDB = conn.getDB('admin');
        adminDB.auth('admin', AUTH_INFO.admin.admin.pwd);
        conn.getDB('test').dropDatabase();
        adminDB.dropDatabase();
    };

    var failures = [];
    setup();
    TESTS.forEach(function(testFunc) {
        try {
            jsTest.log(testFunc.name);

            var newConn = new Mongo(conn.host);
            newConn.host = conn.host;
            testFunc.test(newConn);
        } catch (x) {
            failures.push(testFunc.name);
            jsTestLog(x);
            assert(false, testFunc.name)
        }
    });

    teardown();

    if (failures.length > 0) {
        var list = '';
        failures.forEach(function(test) {
            list += (test + '\n');
        });
        throw Error('Tests failed:\n' + list);
    }
};

var conn = MongoRunner.runMongod({auth: ''});
runTests(conn);
MongoRunner.stopMongod(conn.port);

jsTest.log('Test sharding');
var st = new ShardingTest({shards: 1, keyFile: 'jstests/libs/key1'});
runTests(st.s);
st.stop();

print('SUCCESS! Completed basic_role_auth.js');
