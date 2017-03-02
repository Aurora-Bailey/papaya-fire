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

  // Check for existing records
  userRef.once('value', (snap) => {
    // If user is already set
    if (snap.val()) {
      reject('User is already set!')
    } else {
      // Insert new user
      userRef.set(setUser()).then(() => {
        resolve()
      }, (error) => {
        reject(error)
      })
    }
  }, (error) => {
    reject(error)
  })

});

// Move error to fail path
var error = new Queue(ref, {specId: 'error', 'sanitize': false}, function(data, progress, resolve, reject) {
  // progress(10);

  var failRef = admin.database().ref('queue').child('fail')

  failRef.push(data).then(() => {
    resolve()
  }, (error) => {
    console.log(error)
    resolve()
  })

});

// ref.child('tasks').push({'_state': 'init_user', '_uid': 'E0vy', 'foo': 'bar'});

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
