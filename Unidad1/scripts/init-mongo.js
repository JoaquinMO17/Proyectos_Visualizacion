// MongoDB initialization script
db = db.getSiblingDB('admin');
db.auth('admin', 'admin123');

db = db.getSiblingDB('movies_db');

// Check for JSON files and load them
var jsonFiles = [
  '/docker-entrypoint-initdb.d/data/imdb_movies_final.json',
  '/docker-entrypoint-initdb.d/data/raw/imdb_movies_final.json'
];

var dataLoaded = false;

for (var i = 0; i < jsonFiles.length; i++) {
  try {
    var fileContent = cat(jsonFiles[i]);
    if (fileContent) {
      // Parse JSON Lines format (one JSON object per line)
      var lines = fileContent.split('\n').filter(function(line) {
        return line.trim().length > 0;
      });
      
      var documents = [];
      for (var j = 0; j < lines.length; j++) {
        try {
          var doc = JSON.parse(lines[j]);
          documents.push(doc);
        } catch (e) {
          print('Error parsing line ' + j + ': ' + e);
        }
      }
      
      if (documents.length > 0) {
        db.movies.insertMany(documents);
        print('Loaded ' + documents.length + ' movies from ' + jsonFiles[i]);
        dataLoaded = true;
        break;
      }
    }
  } catch (e) {
    print('File not found or error reading: ' + jsonFiles[i]);
  }
}

if (!dataLoaded) {
  print('Warning: No JSON data files found. MongoDB will start empty.');
  print('Run the ETL process to generate the JSON files.');
}

// Create indexes for better query performance
db.movies.createIndex({ "imdb_title_id": 1 }, { unique: true });
db.movies.createIndex({ "title": 1 });
db.movies.createIndex({ "year": -1 });
db.movies.createIndex({ "avg_vote": -1 });
db.movies.createIndex({ "country": 1 });
db.movies.createIndex({ "director": 1 });

print('MongoDB initialization completed');