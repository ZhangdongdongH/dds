/**
 * Test that user modifications on replica set primaries
 * will invalidate cached user credentials on secondaries
 */

var NUM_NODES = 3;
var rsTest = new ReplSetTest({nodes: NUM_NODES});
rsTest.startSet({oplogSize: 10, keyFile: 'jstests/libs/key1'});
rsTest.initiate();
rsTest.awaitSecondaryNodes();

var primary = rsTest.getPrimary();
var secondary = rsTest.getSecondary();
var admin = primary.getDB('admin');

// Setup initial data
admin.createUser({user: 'admin', pwd: 'Github@12', roles: jsTest.adminUserRoles, "passwordDigestor" : "server"});
admin.auth('admin', 'Github@12');

primary.getDB('foo').createUser({user: 'foo', pwd: 'Github@12', roles: [], "passwordDigestor" : "server"}, {w: NUM_NODES});

secondaryFoo = secondary.getDB('foo');
secondaryFoo.auth('foo', 'Github@12');
assert.throws(function() {
    secondaryFoo.col.findOne();
}, [], "Secondary read worked without permissions");

primary.getDB('foo').updateUser('foo', {roles: jsTest.basicUserRoles}, {w: NUM_NODES});
assert.doesNotThrow(function() {
    secondaryFoo.col.findOne();
}, [], "Secondary read did not work with permissions");

rsTest.stopSet();
