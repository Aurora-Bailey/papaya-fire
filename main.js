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
var geoFire = new GeoFire(dbRef.child('geofire'))

// Pull database for working copy
let userDataCache = null
dbRef.child('user').on('value', snap => {
  userDataCache = snap.val()
})
let userTagsDataCache = null
dbRef.child('userTags').on('value', snap => {
  userTagsDataCache = snap.val()
})
let geofireDataCache = null
dbRef.child('geofire').on('value', snap => {
  geofireDataCache = snap.val()
})

function likeness (meId, youId) {
  let out = {}
  out.tags = {}
  out.yourWeight = 0
  out.myWeight = 0
  out.avgWeight = 0
  let myTags = userTagsDataCache[meId]
  let yourTags = userTagsDataCache[youId]
  let keys = Object.keys(yourTags)
  keys.forEach((tagKey) => {
    if (myTags[tagKey]) {
      // Copy the highlighting of YOUR tags
      out.tags[tagKey] = yourTags[tagKey].level
      out.yourWeight += yourTags[tagKey].weight
      out.myWeight += myTags[tagKey].weight
    }
  })
  out.avgWeight = (out.myWeight + out.yourWeight) / 2
  return out // {tags: {tag: level}, yourWeight, myWeight, avgWeight}
}

// Setup Queues
var findPeople = new Queue(queueRef, {specId: 'find_people', numWorkers: 1, 'sanitize': false}, function(data, progress, resolve, reject) {
  if (!data.watching) { reject('Watching is not set!'); return false }
  if (userDataCache === null) { reject('Firebase userDataCache is not set!'); return false }
  if (userTagsDataCache === null) { reject('Firebase userTagsDataCache is not set!'); return false }
  progress(1)

  let uid = data._uid
  let userMe = userDataCache[uid]
  let myTags = userTagsDataCache[uid]

  // Get user location
  geoFire.get(uid).then(location => {
    if (!location) {
      reject('Provided key is not in GeoFire')
      return false
    }

    // Get people close to user
    let usersInsideRadius = []
    var geoQuery = geoFire.query({
      center: location, // [lat, lng]
      radius: Lib.mileToKilometer(userMe.distance) // Radius in kilometers
    });
    // Fill user array one at a time
    geoQuery.on("key_entered", (key, location, distance) => {
      if (key !== uid) usersInsideRadius.push({key, location, distance})
    })
    // When the array is full
    geoQuery.on("ready", () => {
      geoQuery.cancel() // GeoQuery has loaded and fired all other events for initial data
      progress(25)

      // Calculate matches
      let rebundle = []
      usersInsideRadius.forEach((you, i) => {
        let like = likeness(uid, you.key)
        rebundle[i] = {}
        rebundle[i].uid = you.key
        rebundle[i].weight = Math.round(like.avgWeight)
        rebundle[i].dist = Math.round(you.distance)
      })
      progress(75)

      // Sort
      rebundle.sort((a, b) => {
        if (a.weight < b.weight) return 1
        if (a.weight > b.weight) return -1
        return 0
      })

      // start send
      var outRef = dbRef.child('findPeople').child(uid).child(data.watching)
      outRef.set(rebundle).then(() => {
        // remove after 5 seconds
        // move to separate task
        setTimeout(function() {
          outRef.remove()
        }, 50000)
        resolve()
      }, error => {
        reject(error)
      })
      // end of send
    })
  }, error => {
    reject(error)
  });
  /*
  // output list of people (UID, WeightAverage, WeightMe, WeightYou, CommonTags)
  var outRef = dbRef.child('findPeople').child(data._uid).child(data.watching)
  outRef.set(['One', 'Two', 'Three']).then(() => {
    // remove after 5 seconds
    // move to separate task
    setTimeout(function() {
      outRef.remove()
    }, 5000)
    resolve()
  }, error => {
    reject(error)
  })
  */
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
