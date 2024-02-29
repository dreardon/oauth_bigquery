const express = require("express");
const bodyParser = require('body-parser');
const cookieParser = require('cookie-parser');
const session = require('express-session');
const {BigQuery} = require('@google-cloud/bigquery');
const {OAuth2Client} = require('google-auth-library');
const { google } = require('googleapis');
const url = require('url');
const fetch = (...args) => import('node-fetch').then(({default: fetch}) => fetch(...args));
const { GoogleGenerativeAI,HarmCategory,HarmBlockThreshold } = require("@google/generative-ai");

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

const scopes = 'https://www.googleapis.com/auth/bigquery'

const authorizationUrl = oauth2Client.generateAuthUrl({
  access_type: 'offline',
  scope: scopes,
  include_granted_scopes: true,
  prompt: 'consent',
});

app.get('/',
  function(req, res) {
    if ( typeof req.session.accessToken !== 'undefined' && req.session.accessToken ) {
      res.render('pages/index', {auth_url: ""});
    } else {
      res.render('pages/index', {auth_url: authorizationUrl});
    }
    
  }
);

app.post('/results',
  async function(req, res) {

    const sql_command = req.body.command;
    let ACCESS_TOKEN;
    if (req.session.accessToken) {
      ACCESS_TOKEN = req.session.accessToken;
    } else {
      res.render('pages/index', {auth_url: authorizationUrl});
    }

    const DATASET_ID = process.env.DATASET_ID
    const PROJECT_ID = process.env.PROJECT_ID
    const TABLE_ID = process.env.TABLE_ID
    
    const rows = await queryBigQueryWithSDK(ACCESS_TOKEN,DATASET_ID,PROJECT_ID,TABLE_ID,sql_command) 

    req.session.results = rows; 
    res.render('partials/results', {token: ACCESS_TOKEN, results: rows});
  }
);

async function queryBigQueryWithSDK(ACCESS_TOKEN,DATASET_ID,PROJECT_ID,TABLE_ID,sql_command) {
  const query = sql_command

  const authClient = new OAuth2Client({
    credentials: {
      access_token: ACCESS_TOKEN
    }
  });

  const bigqueryClient = new BigQuery({authClient});

  const options = {
      query: query,
      location: 'US',
      useLegacySql: false,
  };

  const [rows] = await bigqueryClient.query(options);
  console.log('rows:' + JSON.stringify(rows))
  return rows 
}

app.post('/chat', async (req, res) => {

  const MODEL_NAME = "gemini-1.0-pro";
  const API_KEY = process.env.API_KEY
  
  const genAI = new GoogleGenerativeAI(API_KEY);
  const model = genAI.getGenerativeModel({ model: MODEL_NAME });

  const generationConfig = {
    temperature: 0.1,
    maxOutputTokens: 512,
  };

  const safetySettings = [
    {
      category: HarmCategory.HARM_CATEGORY_HARASSMENT,
      threshold: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
    },
    {
      category: HarmCategory.HARM_CATEGORY_HATE_SPEECH,
      threshold: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
    },
    {
      category: HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
      threshold: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
    },
    {
      category: HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
      threshold: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
    },
  ];

  const chat = model.startChat({
    generationConfig,
    safetySettings,
    history: [
    ],
  });
  const user_message = req.body.message;
  const baseline = `You're a model helping people with SQL queries. If your response contains SQL, you must limit your SQL responses to always \
    return the fields unique_id, firstname, lastname, and email coming from the table '${process.env.PROJECT_ID}.${process.env.DATASET_ID}.${process.env.TABLE_ID}' There are no other fields \
    to add to your SQL query. All SQL queries should be wrapped in SQL markdown language. You can also answer questions related to the \
    Human Resource (HR) operations `
  const result = await chat.sendMessage(baseline + user_message);
  const response = result.response;
  res.send(response.text())
})

app.get('/oauth/callback', async (req, res) => {
  let q = url.parse(req.url, true).query;
  try {
    let { tokens } = await oauth2Client.getToken(q.code);
    oauth2Client.setCredentials(tokens);
    req.session.accessToken = tokens.access_token; 
  } catch (e) {
    console.log("Error: " + e)
  }
  res.redirect('/');
});

// Route to clear session
app.get('/logout', (req, res) => {
  req.session.destroy(err => {
    if (err) {
      res.status(500).send("Error clearing session");
    } else {
      console.log('Cleared session')
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