// initialize

var Queue = require('firebase-queue')
var admin = require('firebase-admin')
var GeoFire = require('geofire')
var _ = require('lodash')
var Lib = require('./lib.js')

var serviceAccount = require('./papaya-71cda-firebase-adminsdk-bnb2l-4acfcfac08.json')

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: 'https://papaya-71cda.firebaseio.com'
});

// Setup Refs
var dbRef = admin.database().ref()
var queueRef = dbRef.child('queue')
var geoFireUser = new GeoFire(dbRef.child('geofireUser'))
var geoFireEvent = new GeoFire(dbRef.child('geofireEvent'))

// Pull database for working copy
let userDataCache = null
dbRef.child('user').on('value', snap => {
  userDataCache = snap.val()
})
let userTagsDataCache = null
dbRef.child('userTags').on('value', snap => {
  userTagsDataCache = snap.val()
})
let geofireUserDataCache = null
dbRef.child('geofireUser').on('value', snap => {
  geofireUserDataCache = snap.val()
})
let geofireEventDataCache = null
dbRef.child('geofireEvent').on('value', snap => {
  geofireEventDataCache = snap.val()
})
let eventDataCache = null
dbRef.child('event').on('value', snap => {
  eventDataCache = snap.val()
})
let eventAuthDataCache = null
dbRef.child('eventAuth').on('value', snap => {
  eventAuthDataCache = snap.val()
})

function likeness (meId, youId) {
  let out = {}
  out.tags = []
  out.yourWeight = 0 // Weight of YOUR matching tags
  out.myWeight = 0 // Weight of MY matching tags
  if (!userTagsDataCache[youId]) return out // Just in case a bad uid is provided
  if (!userTagsDataCache[meId]) return out // Just in case you don't have any tags
  let myTags = userTagsDataCache[meId]
  let yourTags = userTagsDataCache[youId]
  let keys = Object.keys(yourTags)
  keys.forEach((tagKey) => {
    if (myTags[tagKey]) {
      // Copy the highlighting of YOUR tags
      out.tags.push({n: tagKey, w: yourTags[tagKey].weight, l: yourTags[tagKey].level})
      out.yourWeight += yourTags[tagKey].weight
      out.myWeight += myTags[tagKey].weight
    }
  })
  return out // {tags: {tag: level}, yourWeight, myWeight}
  // not calculating total weight because it punishes people with lots of tags
}

// Setup Queues
var findPeople = new Queue(queueRef, {specId: 'find_events', numWorkers: 1, 'sanitize': false}, function(data, progress, resolve, reject) {
  if (!data.watching) { reject('Watching is not set!'); return false }
  if (userDataCache === null) { reject('Firebase userDataCache is not set!'); return false }
  if (userTagsDataCache === null) { reject('Firebase userTagsDataCache is not set!'); return false }
  if (geofireEventDataCache === null) { reject('Firebase geofireEventDataCache is not set!'); return false }
  if (eventDataCache === null) { reject('Firebase eventDataCache is not set!'); return false }
  // temp if (eventAuthDataCache === null) { reject('Firebase eventAuthDataCache is not set!'); return false }
  progress(1)

  let uid = data._uid
  let userMe = userDataCache[uid]
  let myTags = userTagsDataCache[uid]

  // Get user location
  geoFireUser.get(uid).then(location => {
    if (!location) {
      reject('Provided key is not in GeoFire')
      return false
    }

    // Get events close to user
    let eventsInsideRadius = []
    var geoQuery = geoFireEvent.query({
      center: location, // [lat, lng]
      radius: Lib.mileToKilometer(userMe.distance) // Radius in kilometers
    });
    // Called once for each key in the area
    geoQuery.on("key_entered", (key, location, distance) => {
      eventsInsideRadius.push({key, location, distance})
    })
    // When all keys have been called
    geoQuery.on("ready", () => {
      geoQuery.cancel() // GeoQuery has loaded and fired all other events for initial data
      progress(25)

      // Calculate event stuff
      let rebundle = []
      eventsInsideRadius.forEach((event, i) => {
        // let like = likeness(uid, you.key)
        rebundle[i] = {}
        rebundle[i].eid = event.key
        //rebundle[i].yw = like.yourWeight
        //rebundle[i].mw = like.myWeight
        //rebundle[i].tags = like.tags
        rebundle[i].dist = Math.round(event.distance)
      })
      progress(75)

      // Sort
      rebundle.sort((a, b) => {
        if (a.eid < b.eid) return 1
        if (a.eid > b.eid) return -1
        return 0
      })

      // Output to watching location
      if (rebundle.length === 0) rebundle = 'empty'
      var outRef = dbRef.child('computed').child(uid).child(data.watching)
      outRef.set(rebundle).then(() => {
        // remove after 50 seconds
        // move to separate task?
        setTimeout(() => {
          outRef.remove()
        }, 50000)
        // Task resolves. (Timeout will trigger 50 seconds after resolve)
        resolve()
      }, error => {
        reject(error)
      })
      // end of send
    })
  }, error => {
    reject(error)
  });

})
var findPeople = new Queue(queueRef, {specId: 'find_people', numWorkers: 1, 'sanitize': false}, function(data, progress, resolve, reject) {
  if (!data.watching) { reject('Watching is not set!'); return false }
  if (userDataCache === null) { reject('Firebase userDataCache is not set!'); return false }
  if (userTagsDataCache === null) { reject('Firebase userTagsDataCache is not set!'); return false }
  if (geofireUserDataCache === null) { reject('Firebase geofireUserDataCache is not set!'); return false }
  progress(1)

  let uid = data._uid
  let userMe = userDataCache[uid]
  let myTags = userTagsDataCache[uid]

  // Get user location
  geoFireUser.get(uid).then(location => {
    if (!location) {
      reject('Provided key is not in GeoFire')
      return false
    }

    // Get people close to user
    let usersInsideRadius = []
    var geoQuery = geoFireUser.query({
      center: location, // [lat, lng]
      radius: Lib.mileToKilometer(userMe.distance) // Radius in kilometers
    });
    // Called once for each key in the area
    geoQuery.on("key_entered", (key, location, distance) => {
      if (key !== uid) usersInsideRadius.push({key, location, distance})
    })
    // When all keys have been called
    geoQuery.on("ready", () => {
      geoQuery.cancel() // GeoQuery has loaded and fired all other events for initial data
      progress(25)

      // Calculate matches
      let rebundle = []
      usersInsideRadius.forEach((you, i) => {
        let like = likeness(uid, you.key)
        rebundle[i] = {}
        rebundle[i].uid = you.key
        rebundle[i].yw = like.yourWeight
        rebundle[i].mw = like.myWeight
        rebundle[i].tags = like.tags
        rebundle[i].dist = Math.round(you.distance)
      })
      progress(75)

      // Sort
      // My weight, or the things I find important are highest priority
      rebundle.sort((a, b) => {
        if (a.mw < b.mw) return 1
        if (a.mw > b.mw) return -1
        return 0
      })

      // Output to watching location
      if (rebundle.length === 0) rebundle = 'empty'
      var outRef = dbRef.child('computed').child(uid).child(data.watching)
      outRef.set(rebundle).then(() => {
        // remove after 50 seconds
        // move to separate task?
        setTimeout(() => {
          outRef.remove()
        }, 50000)
        // Task resolves. (Timeout will trigger 50 seconds after resolve)
        resolve()
      }, error => {
        reject(error)
      })
      // end of send
    })
  }, error => {
    reject(error)
  });

})
var profilePeople = new Queue(queueRef, {specId: 'profile_people', numWorkers: 1, 'sanitize': false}, function(data, progress, resolve, reject) {
  if (!data.watching) { reject('Watching is not set!'); return false }
  if (typeof data.list === 'undefined') { reject('List is not set!'); return false }
  if (userDataCache === null) { reject('Firebase userDataCache is not set!'); return false }
  if (userTagsDataCache === null) { reject('Firebase userTagsDataCache is not set!'); return false }
  if (geofireUserDataCache === null) { reject('Firebase geofireUserDataCache is not set!'); return false }
  progress(1)

  let uid = data._uid
  let userMe = userDataCache[uid]
  let myTags = userTagsDataCache[uid]

  let rebundle = []
  let people = data.list.split(',')
  people.forEach((person, i) => {
    let like = likeness(uid, person)
    rebundle[i] = {}
    rebundle[i].uid = person
    rebundle[i].yw = like.yourWeight
    rebundle[i].mw = like.myWeight
    rebundle[i].tags = like.tags
    rebundle[i].dist = 0
  })
  progress(75)

  // Sort
  // My weight, or the things I find important are highest priority
  rebundle.sort((a, b) => {
    if (a.mw < b.mw) return 1
    if (a.mw > b.mw) return -1
    return 0
  })

  // Output to watching location
  if (data.list === '') rebundle = 'empty'
  var outRef = dbRef.child('computed').child(uid).child(data.watching)
  outRef.set(rebundle).then(() => {
    // remove after 50 seconds
    // move to separate task?
    setTimeout(() => {
      outRef.remove()
    }, 50000)
    // Task resolves. (Timeout will trigger 50 seconds after resolve)
    resolve()
  }, error => {
    reject(error)
  })
  // end of send
})

// Move error to fail path
// start on _error
var error = new Queue(queueRef, {specId: 'error', 'sanitize': false}, function(data, progress, resolve, reject) {
  // Write the failed task to the database
  var failRef = dbRef.child('queue').child('fail')
  failRef.push(data).then(() => {
    resolve()
  }, (error) => {
    console.log(error)
    resolve()
  })
})

// Move error to crit path
// start on _crit
var crit = new Queue(queueRef, {specId: 'crit', 'sanitize': false}, function(data, progress, resolve, reject) {
  // Write the failed task to the database
  var critRef = dbRef.child('queue').child('crit')
  critRef.push(data).then(() => {
    resolve()
  }, (error) => {
    console.log(error)
    resolve()
  })
})

// Ready
console.log('Listening...')
