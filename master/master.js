const rp = require('request-promise');
const fs = require('fs');

// Size of a batch in bytes.
const batchSize = 5;

// Replication. Defines how many time a batch should be replicated among slaves.
const magicNumber = 2;

/*
It maps filenames to an array of the location of each of the batches. Position n
in the array specifies the indexes of the slaves that contain the batch number n.
{
  "sample.txt": [
    [0, 2],
    [0, 1],
  ]
}
*/
let files = {}

/*
Slaves dictionary. It stores information about slaves such as their ip address
and the ammount of batches currently stored in them.
The format is as follows:

{
  index: 3
  address: "localhost:3000",
  batchesCount: 2,
}
*/
let slaves = [
  {index: 0, address: "http://localhost:4001", batchesCount: 0},
  {index: 1, address: "http://localhost:4002", batchesCount: 0},
  {index: 2, address: "http://localhost:4003", batchesCount: 0},
  {index: 3, address: "http://localhost:4004", batchesCount: 0}
];

// Utility function to get the slave with more available memory.
function getMinSlaveIndex(){
  let tmp = 0;
  for (let i = 1; i < slaves.length; i++) {
    if(slaves[i].batchesCount < slaves[tmp].batchesCount) {
      tmp = i;
    }
  }
  return tmp
}

// ------- PUT file to distributed file system functionality ---------

// Distributes the given file separated in batches over the entire slaves system.
async function distributeFile(fileName, buffer) {
  console.log("[Master]: Distributing file " + fileName)
  files[fileName] = []
  let size = buffer.length;
  let index = 0;
  let i = 0;

  while(size > batchSize) {
    // Store first
    let batchSlice = buffer.slice(index, index + batchSize);
    await distributeBatch(fileName, batchSlice, i)
    index += batchSize;
    i++;
    size -= batchSize;
  }

  let batchSlice = buffer.slice(index, - 1);
  await distributeBatch(fileName, batchSlice, i)

  console.log(files)
}

// Distributes a single batch of a file in a defined amount of slaves.
async function distributeBatch(fileName, batchSlice, batchIndex) {
  console.log("[Master]: Distributing batch " + batchSlice + " of file " + fileName);
  let batchLocations = []

  for(let i = 0; i < magicNumber; i++) {
    slaveIndex = getMinSlaveIndex();
    if (await sendBatchToSlave(fileName, batchSlice, batchIndex, slaveIndex) ) {
      slaves[slaveIndex].batchesCount++;
      batchLocations.push(slaveIndex);
    } else {

    }
  }

  files[fileName].push(batchLocations)
}

// It sends a single batch to a specific slave.
async function sendBatchToSlave(fileName, batchSlice, batchIndex, slaveIndex) {
  console.log("Slave: ", slaveIndex, slaves[slaveIndex])
  console.log("Sending batch " + batchSlice + " to slave number " + slaveIndex)

  let payload = {
    file: fileName,
    batchIndex: batchIndex,
    data: batchSlice
  }

  console.log("Sending:" , payload)
  const requestOptions = {
    method: 'POST',
    uri: slaves[slaveIndex].address + "/file",
    body: payload,
    json: true
  };

  let slaveResponse;
  try {
    slaveResponse = await rp(requestOptions);
  } catch (err) {
    console.error("Error sending to slave: " + err.message);
    return null
  }

  return slaveResponse;
}

// ------- GET file to distributed file system functionality ---------

// It retrieves all pieces of the given file in the system and builds it locally.
async function getDistributedFile(fileName) {

  // Iterate through all file batches.
  for(let i = 0; i < files[fileName].length; i++) {
    let batch = await getBatch(fileName, i)
    if(!batch) {
      console.error(`Could not retrieve batch number ${i} from file "${fileName}"`)
      return null
    }
    fs.appendFileSync(fileName, Buffer.from(batch.data).toString("utf8"))
  }

  return true;
}

// It searches a single batch between the slaves we know that contains that batch.
async function getBatch(fileName, index) {
  let locations = files[fileName][index];
  let batch;

  for (let i = 0; i < locations.length; i++) {
    batch = await getBatchFromSlave(locations[i], fileName, index);
    if(batch) {
      return batch;
    }
    return null
  }
}

// It retrieves a batch from a specific slave.
async function getBatchFromSlave(slaveIndex, fileName,index) {
  const requestOptions = {
    method: 'POST',
    uri: `${slaves[slaveIndex].address}/getBatch`,
    body: {file: fileName, batchIndex: index},
    json: true
  };

  let slaveResponse;
  try {
    slaveResponse = await rp(requestOptions);
  } catch (err) {
    console.error(`Error getting batch ${index} of file "${fileName}" from slave number ${slaveIndex}`);
    console.error(err.error)
    return null
  }

  return slaveResponse;
}


module.exports = {distributeFile, getDistributedFile}
