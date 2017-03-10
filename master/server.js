const express = require('express');
const rp = require('request-promise');
const multer  = require('multer');
const fs = require('fs');
const bodyParser = require('body-parser');
const master = require('./master')

// Setup express app server.
const upload = multer({ dest: 'uploads/' })
const app = express();
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

// Endpoint to upload file.
app.post('/file', upload.single('file'), function (req, res, next) {
  const {path, originalname} = req.file;
  let fileContent = fs.readFileSync(path)
  master.distributeFile(originalname, fileContent).catch(console.error)
  fs.unlinkSync(path);
  res.send("OK");
})

// Enpoint to download file.
app.get('/file/:fileName', async function (req, res) {
  let path = __dirname + "/uploads/" + req.params.fileName
  let ok = await master.getDistributedFile(path)
  res.sendFile(path);
  fs.unlinkSync(path);
});

// Hello endpoint.
app.get('/', function(req, res) {
  res.send("Distributed File System Master Node");
});

// Turn on server.
app.listen(4000, function () {
  console.log('Master node running on port 4000!');
});
