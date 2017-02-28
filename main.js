// initialize

var Queue = require('firebase-queue');
var admin = require('firebase-admin');

var serviceAccount = require('./papaya-71cda-firebase-adminsdk-bnb2l-4acfcfac08.json');

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: 'https://papaya-71cda.firebaseio.com'
});

// Setup Queue

let process_count = 0;

var ref = admin.database().ref('queue');
var options = {
  'specId': 'build_profile',
  'numWorkers': 5
};
var queue = new Queue(ref, function(data, progress, resolve, reject) {
  // Read and process task data
  console.log(process_count, data);
  process_count++;

  // Do some work
  progress(50);

  // Finish the task asynchronously
  setTimeout(function() {
    resolve();
  }, 5000);
});


ref.child('tasks').push({'foo': 'bar'});
ref.child('tasks').push({'foo': 'bar'});
ref.child('tasks').push({'foo': 'bar'});
ref.child('tasks').push({'foo': 'bar'});
ref.child('tasks').push({'foo': 'bar'});

ref.child('tasks').push({'foo': 'bar'});
ref.child('tasks').push({'foo': 'bar'});
ref.child('tasks').push({'foo': 'bar'});
ref.child('tasks').push({'foo': 'bar'});
ref.child('tasks').push({'foo': 'bar'});

ref.child('tasks').push({'foo': 'bar'});


console.log('setup')
