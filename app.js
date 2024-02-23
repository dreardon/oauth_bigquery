const express = require("express");
const bodyParser = require('body-parser');
const cookieParser = require('cookie-parser');
const session = require('express-session');
const {BigQuery} = require('@google-cloud/bigquery');
const { google } = require('googleapis');
const url = require('url');
const fetch = (...args) => import('node-fetch').then(({default: fetch}) => fetch(...args));

require('dotenv').config()

var app = express();
app.set('view engine', 'ejs');
app.use(express.static(__dirname + '/static'));

app.use(cookieParser());
app.use(bodyParser.urlencoded({ extended: false }))
app.use(bodyParser.json())
app.use(session({secret: process.env.SESSION_SECRET, resave: true, saveUninitialized: true}));


const oauth2Client = new google.auth.OAuth2(
  process.env.CLIENT_ID,
  process.env.CLIENT_SECRET,
  process.env.CALLBACK_URL,
);

const scopes = 'https://www.googleapis.com/auth/bigquery.readonly'

const authorizationUrl = oauth2Client.generateAuthUrl({
  access_type: 'offline',
  scope: scopes,
  include_granted_scopes: true,
  prompt: 'consent',
});


function transformResponse(response) {
  const fieldNames = ['unique_id', 'firstname', 'lastname', 'email'];

  return response.map(row => {
    const rowObject = {};
    row.f.forEach((field, index) => {
      rowObject[fieldNames[index]] = field.v;
    });
    return rowObject;
  });
}

app.get('/',
  function(req, res) {
    res.render('pages/index', {auth_url: authorizationUrl});
  }
);

app.get('/results',
  async function(req, res) {
    const ACCESS_TOKEN = req.session.accessToken;
    console.log('ACCESS_TOKEN:' + ACCESS_TOKEN)

    const DATASET_ID = process.env.DATASET_ID
    const PROJECT_ID = process.env.PROJECT_ID
    const TABLE_ID = process.env.TABLE_ID

    const rows = await queryBigQueryWithREST(ACCESS_TOKEN,DATASET_ID,PROJECT_ID,TABLE_ID)
    //const rows_sdk = await queryBigQueryWithSDK(ACCESS_TOKEN,DATASET_ID,PROJECT_ID,TABLE_ID) 

    console.log('Returned Rows: ' + JSON.stringify(rows))
    //console.log('Returned Rows_sdk: ' + JSON.stringify(rows_sdk))
    res.render('pages/results', {token: ACCESS_TOKEN, results: rows });
  }
);

async function queryBigQueryWithREST(ACCESS_TOKEN,DATASET_ID,PROJECT_ID,TABLE_ID) {
  const query = `SELECT unique_id, firstname, lastname, email FROM \`${PROJECT_ID}.${DATASET_ID}.${TABLE_ID}\``;
  const url = `https://bigquery.googleapis.com/bigquery/v2/projects/${PROJECT_ID}/queries`;
  const options = {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${ACCESS_TOKEN}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      query: query,
      location: 'US',
      useLegacySql: false
    })
  };

  try {
    const response = await fetch(url, options);
    const data = await response.json();
    return transformResponse(data.rows) 
  } catch (error) {
      console.error('Error running query:', error);
    throw error;
  }
}

async function queryBigQueryWithSDK(ACCESS_TOKEN,DATASET_ID,PROJECT_ID,TABLE_ID) {
  const query = `SELECT unique_id, firstname, lastname, email FROM \`${PROJECT_ID}.${DATASET_ID}.${TABLE_ID}\``;
  
  const bigqueryClient = new BigQuery({
  projectId: `${PROJECT_ID}`});

  const options = {
      query: query,
      location: 'US',
      useLegacySql: false,
      headers: {
          'Authorization': `Bearer ${ACCESS_TOKEN}`,
      }
  };
  console.log(JSON.stringify(options))

  const [rows] = await bigqueryClient.query(options);

  console.log('Query Results:');
  rows.forEach(row => {
    console.log(`row: ${row}`);
  });
}

app.get('/oauth/callback', async (req, res) => {
  //console.log('Full OAuth Callback URL ' + req.url)
  let q = url.parse(req.url, true).query;
  let { tokens } = await oauth2Client.getToken(q.code);
  oauth2Client.setCredentials(tokens);
  //console.log("All Tokens Server-side: " + JSON.stringify(tokens))
  //console.log("Access Token Server-side: " + tokens.access_token)
  req.session.accessToken = tokens.access_token; 
  res.redirect('/results');
});

// Route to clear session
app.get('/logout', (req, res) => {
  req.session.destroy(err => {
    if (err) {
      res.status(500).send("Error clearing session");
    } else {
      res.redirect('/');
    }
  }); 
});

//general handlers
app.use(function(err, req, res, next) {
  console.log("Fatal error: " + JSON.stringify(err));
  next(err);
});

var server = app.listen(8080, function () {
  console.log('Listening on port %d', server.address().port)
});