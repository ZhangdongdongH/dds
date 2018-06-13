// dumprestore_auth.js

t = new ToolTest("dumprestore_auth", {auth: ""});

c = t.startDB("foo");
var dbName = c.getDB().toString();
print("DB is ", dbName);

adminDB = c.getDB().getSiblingDB('admin');
adminDB.createUser({user: 'admin', pwd: 'Github@12', roles: ['root'], "passwordDigestor" : "server"});
adminDB.auth('admin', 'Github@12');
adminDB.createUser({user: 'backup', pwd: 'Github@12', roles: ['backup'], "passwordDigestor" : "server"});
adminDB.createUser({user: 'restore', pwd: 'Github@12', roles: ['restore'], "passwordDigestor" : "server"});

// Add user defined roles & users with those roles
var testUserAdmin = c.getDB().getSiblingDB(dbName);
var backupActions = ["find", "listCollections", "listIndexes"];
testUserAdmin.createRole({
    role: "backupFoo",
    privileges: [
        {resource: {db: dbName, collection: "foo"}, actions: backupActions},
        {resource: {db: dbName, collection: ""}, actions: backupActions}
    ],
    roles: []
});
testUserAdmin.createUser({user: 'backupFoo', pwd: 'Github@12', roles: ['backupFoo'], "passwordDigestor" : "server"});

var restoreActions = [
    "collMod",
    "createCollection",
    "createIndex",
    "dropCollection",
    "insert",
    "listCollections",
    "listIndexes"
];
var restoreActionsFind = restoreActions;
restoreActionsFind.push("find");
testUserAdmin.createRole({
    role: "restoreChester",
    privileges: [
        {resource: {db: dbName, collection: "chester"}, actions: restoreActions},
        {resource: {db: dbName, collection: ""}, actions: ["listCollections", "listIndexes"]},
    ],
    roles: []
});
testUserAdmin.createRole({
    role: "restoreFoo",
    privileges: [
        {resource: {db: dbName, collection: "foo"}, actions: restoreActions},
        {resource: {db: dbName, collection: ""}, actions: ["listCollections", "listIndexes"]},
    ],
    roles: []
});
testUserAdmin.createUser({user: 'restoreChester', pwd: 'Github@12', roles: ['restoreChester'], "passwordDigestor" : "server"});
testUserAdmin.createUser({user: 'restoreFoo', pwd: 'Github@12', roles: ['restoreFoo'], "passwordDigestor" : "server"});

var sysUsers = adminDB.system.users.count();
assert.eq(0, c.count(), "setup1");
c.save({a: 22});
assert.eq(1, c.count(), "setup2");

assert.commandWorked(c.runCommand("collMod", {usePowerOf2Sizes: false}));

var collections = c.getDB().getCollectionInfos();
var fooColl = null;
collections.forEach(function(coll) {
    if (coll.name === "foo") {
        fooColl = coll;
    }
});
assert.neq(null, fooColl, "foo collection doesn't exist");
assert(!fooColl.options.flags, "find namespaces 1");

t.runTool("dump", "--out", t.ext, "--username", "backup", "--password", "Github@12");

c.drop();
assert.eq(0, c.count(), "after drop");

// Restore should fail without user & pass
t.runTool("restore", "--dir", t.ext, "--writeConcern", "0");
assert.eq(0, c.count(), "after restore without auth");

// Restore should pass with authorized user
t.runTool("restore",
          "--dir",
          t.ext,
          "--username",
          "restore",
          "--password",
          "Github@12",
          "--writeConcern",
          "0");
assert.soon("c.findOne()", "no data after sleep");
assert.eq(1, c.count(), "after restore 2");
assert.eq(22, c.findOne().a, "after restore 2");

collections = c.getDB().getCollectionInfos();
fooColl = null;
collections.forEach(function(coll) {
    if (coll.name === "foo") {
        fooColl = coll;
    }
});
assert.neq(null, fooColl, "foo collection doesn't exist");
assert(!fooColl.options.flags, "find namespaces 2");

assert.eq(sysUsers, adminDB.system.users.count());

// Dump & restore DB/colection with user defined roles
t.runTool("dump",
          "--out",
          t.ext,
          "--username",
          "backupFoo",
          "--password",
          "Github@12",
          "--db",
          dbName,
          "--collection",
          "foo");

c.drop();
assert.eq(0, c.count(), "after drop");

// Restore with wrong user
t.runTool("restore",
          "--username",
          "restoreChester",
          "--password",
          "Github@12",
          "--db",
          dbName,
          "--collection",
          "foo",
          t.ext + dbName + "/foo.bson",
          "--writeConcern",
          "0");
assert.eq(0, c.count(), "after restore with wrong user");

// Restore with proper user
t.runTool("restore",
          "--username",
          "restoreFoo",
          "--password",
          "Github@12",
          "--db",
          dbName,
          "--collection",
          "foo",
          t.ext + dbName + "/foo.bson",
          "--writeConcern",
          "0");
assert.soon("c.findOne()", "no data after sleep");
assert.eq(1, c.count(), "after restore 3");
assert.eq(22, c.findOne().a, "after restore 3");

collections = c.getDB().getCollectionInfos();
fooColl = null;
collections.forEach(function(coll) {
    if (coll.name === "foo") {
        fooColl = coll;
    }
});
assert.neq(null, fooColl, "foo collection doesn't exist");
assert(!fooColl.options.flags, "find namespaces 3");

assert.eq(sysUsers, adminDB.system.users.count());

t.stop();
