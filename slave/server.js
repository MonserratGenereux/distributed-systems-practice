var express = require('express')
var bodyParser = require('body-parser');

var app = express()

app.use(bodyParser.json()); // for parsing application/json
app.use(bodyParser.urlencoded({ extended: true })); // for parsing application/x-www-form-urlencoded

/*
Buffers
{
  "text.txt" : {
    0: Buffer < 12234 >,
    3: Buffer < 1234 af>
  }

}
*/
let memory = {}

app.post('/getBatch', function (req, res) {
  let {file, batchIndex} = req.body;
  console.log(`Serving batch ${batchIndex} from file ${file}`);
  console.log(memory[file][batchIndex]);
  res.json(memory[file][batchIndex]);
});

app.post('/file', function (req, res) {
  let {file, batchIndex, data} = req.body;

  if (!memory[file]) {
    memory[file] = {};
  }

  memory[file][batchIndex] = data;

  console.log(`Received batch number ${batchIndex} of file ${file}: ${data}`)
  console.log('Current memory:', memory)
  res.send('OK');
});

app.listen(4004, function () {
  console.log('Slave node running on port 4004!')
});
