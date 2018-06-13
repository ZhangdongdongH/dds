load('jstests/libs/write_concern_util.js');
load('jstests/multiVersion/libs/auth_helpers.js');

/**
 * This file tests that user management commands accept write concern and wait for it properly.
 * It tests that an invalid write concern leads to a writeConcernError, as well as an ok status
 * of 0 to support backward compatibility. It also tests that a valid write concern works and does
 * not yield any writeConcern errors.
 */

(function() {
    "use strict";
    var replTest = new ReplSetTest(
        {name: 'UserManagementWCSet', nodes: 3, settings: {chainingAllowed: false}});
    replTest.startSet();
    replTest.initiate();

    var master = replTest.getPrimary();
    var dbName = "user-management-wc-test";
    var db = master.getDB(dbName);
    var adminDB = master.getDB('admin');

    function dropUsersAndRoles() {
        db.dropUser('username');
        db.dropUser('user1');
        db.dropUser('user2');
    }

    var commands = [];

    commands.push({
        req: {createUser: 'username', pwd: 'Github@12', roles: jsTest.basicUserRoles, "digestPassword" : true},
        setupFunc: function() {},
        confirmFunc: function() {
            assert(db.auth("username", "Github@12"), "auth failed");
            assert(!db.auth("username", "Github@12a"), "auth should have failed");
        },
        admin: false
    });

    commands.push({
        req: {updateUser: 'username', pwd: 'Github@13', roles: jsTest.basicUserRoles, "digestPassword" : true},
        setupFunc: function() {
            db.runCommand({createUser: 'username', pwd: 'Github@12', roles: jsTest.basicUserRoles});
        },
        confirmFunc: function() {
            assert(db.auth("username", "Github@13"), "auth failed");
            assert(!db.auth("username", "Github@12"), "auth should have failed");
        },
        admin: false
    });

    commands.push({
        req: {dropUser: 'tempUser'},
        setupFunc: function() {
            db.runCommand({createUser: 'tempUser', pwd: 'Github@12', roles: jsTest.basicUserRoles, "digestPassword" : true});
            assert(db.auth("tempUser", "Github@12"), "auth failed");
        },
        confirmFunc: function() {
            assert(!db.auth("tempUser", "Github@12"), "auth should have failed");
        },
        admin: false
    });

    commands.push({
        req: {authSchemaUpgrade: 1},
        setupFunc: function() {
            adminDB.system.version.update(
                {_id: "authSchema"}, {"currentVersion": 3}, {upsert: true});

            db.createUser({user: 'user1', pwd: 'Github@12', roles: jsTest.basicUserRoles, "passwordDigestor" : "server"});
            assert(db.auth({mechanism: 'MONGODB-CR', user: 'user1', pwd: 'Github@12'}));

            db.createUser({user: 'user2', pwd: 'Github@12', roles: jsTest.basicUserRoles, "passwordDigestor" : "server"});
            assert(db.auth({mechanism: 'MONGODB-CR', user: 'user2', pwd: 'Github@12'}));
        },
        confirmFunc: function() {
            // All users should only have SCRAM credentials.
            verifyUserDoc(db, 'user1', false, true);
            verifyUserDoc(db, 'user2', false, true);

            // After authSchemaUpgrade MONGODB-CR no longer works.
            verifyAuth(db, 'user1', 'Github@12', false, true);
            verifyAuth(db, 'user2', 'Github@12', false, true);
        },
        admin: true
    });

    commands.push({
        req: {
            _mergeAuthzCollections: 1,
            tempUsersCollection: 'admin.tempusers',
            tempRolesCollection: 'admin.temproles',
            db: "",
            drop: false
        },
        setupFunc: function() {
            adminDB.system.users.remove({});
            adminDB.system.roles.remove({});
            adminDB.createUser({user: 'lorax', pwd: 'Github@12', roles: ['read'], "passwordDigestor" : "server"});
            adminDB.createRole({role: 'role1', roles: ['read'], privileges: []});
            adminDB.system.users.find().forEach(function(doc) {
                adminDB.tempusers.insert(doc);
            });
            adminDB.system.roles.find().forEach(function(doc) {
                adminDB.temproles.insert(doc);
            });
            adminDB.system.users.remove({});
            adminDB.system.roles.remove({});

            assert.eq(0, adminDB.system.users.find().itcount());
            assert.eq(0, adminDB.system.roles.find().itcount());

            db.createUser({user: 'lorax2', pwd: 'Github@12', roles: ['readWrite'], "passwordDigestor" : "server"});
            db.createRole({role: 'role2', roles: ['readWrite'], privileges: []});

            assert.eq(1, adminDB.system.users.find().itcount());
            assert.eq(1, adminDB.system.roles.find().itcount());
        },
        confirmFunc: function() {
            assert.eq(2, adminDB.system.users.find().itcount());
            assert.eq(2, adminDB.system.roles.find().itcount());
        },
        admin: true
    });

    function assertUserManagementWriteConcernError(res) {
        assert(!res.ok);
        assert(res.errmsg);
        assert(res.code);
        assertWriteConcernError(res);
    }

    function testValidWriteConcern(cmd) {
        cmd.req.writeConcern = {w: 'majority', wtimeout: 25000};
        jsTest.log("Testing " + tojson(cmd.req));

        dropUsersAndRoles();
        cmd.setupFunc();
        var res = runCommandCheckAdmin(db, cmd);
        assert.commandWorked(res);
        assert(!res.writeConcernError,
               'command on a full replicaset had writeConcernError: ' + tojson(res));
        cmd.confirmFunc();
    }

    function testInvalidWriteConcern(cmd) {
        cmd.req.writeConcern = {w: 15};
        jsTest.log("Testing " + tojson(cmd.req));

        dropUsersAndRoles();
        cmd.setupFunc();
        var res = runCommandCheckAdmin(db, cmd);
        assertUserManagementWriteConcernError(res);
        cmd.confirmFunc();
    }

    commands.forEach(function(cmd) {
        testValidWriteConcern(cmd);
        testInvalidWriteConcern(cmd);
    });

})();
