// initialize

var Queue = require('firebase-queue');
var admin = require('firebase-admin');

var serviceAccount = require('./papaya-71cda-firebase-adminsdk-bnb2l-4acfcfac08.json');

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: 'https://papaya-71cda.firebaseio.com'
});

// Setup Queue
var ref = admin.database().ref('queue');

// Add user to database
var initUser = new Queue(ref, {specId: 'init_user', 'sanitize': false}, function(data, progress, resolve, reject) {
  // progress(10);

  var userRef = admin.database().ref('user').child(data._uid)
  var authRef = admin.auth()


  // Get user auth for email address
  authRef.getUser(data._uid)
  .then((userAuth) => {
    // Check if the user has already been initialized
    userRef.once('value')
    .then((snap) => {
      if (snap.val()) {
        // If user is already set
        reject('User is already initialized!')
      } else {
        // User not set
        // Insert new user
        let newUser = setUser()
        newUser.email = userAuth.email
        userRef.set(newUser)
        .then(() => {
          resolve()
        })
        .catch((error) => {
          reject(error)
        })
      }
    })
    .catch((error) => {
      reject(error)
    })
  })
  .catch((error) => {
    reject(error)
  });


});

// Move error to fail path
var error = new Queue(ref, {specId: 'error', 'sanitize': false}, function(data, progress, resolve, reject) {
  // Write the failed task to the database
  var failRef = admin.database().ref('queue').child('fail')
  failRef.push(data).then(() => {
    resolve()
  }, (error) => {
    console.log(error)
    resolve()
  })
});

// Move error to crit path
var crit = new Queue(ref, {specId: 'crit', 'sanitize': false}, function(data, progress, resolve, reject) {
  // Write the failed task to the database
  var critRef = admin.database().ref('queue').child('crit')
  critRef.push(data).then(() => {
    resolve()
  }, (error) => {
    console.log(error)
    resolve()
  })
});

console.log('Listening...')

function setUser () {
  return {
    locationName: 'Not Set',
    locationLong: 0,
    locationLat: 0,
    displayName: '',
    pictureURL: '',
    firstName: '',
    lastName: '',
    distance: 20,
    birthday: Date.now(), // Timestamp
    email: '',
    bio: '',
    sex: 'na'
  }
}
