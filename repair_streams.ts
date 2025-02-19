import fs from "fs";
import csv from "csv-parser";
import axios from "axios";
import {create as createIPFSClient } from "ipfs-http-client";
import { v4 as uuidv4 } from 'uuid';
import { DID } from 'dids';
import { Ed25519Provider } from 'key-did-provider-ed25519';
import { getResolver } from 'key-did-resolver';
import { fromString } from 'uint8arrays';
import {type CAR, CARFactory } from 'cartonne'
 import { CommitID, StreamID} from '@ceramicnetwork/streamid';
import { CeramicClient } from "@ceramicnetwork/http-client";

// const CAS_URL = "https://cas-dev-direct.3boxlabs.com"; // Replace with the actual CAS URL
const CAS_URL = "https://cas.3boxlabs.com";
const IPFS_NODE_URL = 'http://ceramic-one-0:5101'; 

const cf = new CARFactory();

interface CasAuthPayload {
  url: string;
  nonce: string;
  digest: string;
}

async function createAuthHeader(url: string, digest: string): Promise<string> {
  const authPayload: CasAuthPayload = {
    url,
    nonce: uuidv4(),
    digest,
  };

  const nodePrivateKey = process.env.NODE_PRIVATE_KEY;
  if(!nodePrivateKey) {
    console.error('Node private key not found');
    return '';
  }
  const seed = fromString(nodePrivateKey, 'hex');
  
  const did = new DID({
    provider: new Ed25519Provider(seed),
    resolver: getResolver(),
  });
  await did.authenticate();

  const jws = await did.createJWS(authPayload);
  const protectedHeader = jws.signatures[0].protected;
  const signature = jws.signatures[0].signature;
  return `Bearer ${protectedHeader}.${jws.payload}.${signature}`;
}

interface StreamData {
  streamId: string;
}

// Function to create IPFS client
function createIPFSClientInstance() {
  return createIPFSClient({
    url: IPFS_NODE_URL,
  });
}


async function uploadToIPFS(carFile: Uint8Array): Promise<void> {
  const ipfs = createIPFSClientInstance();
  const bytes = cf.fromBytes(carFile);
  console.log(bytes.roots);
  try {
    // add sleep for 1 second
    const result = ipfs.dag.import([carFile], {pinRoots : false});
    for await (const res of result) {
      console.log('Successfully stored car to IPFS:',res);
    }
    console.log('This is the car stored:', await ipfs.dag.get(bytes.roots[0]));
  } catch (e) {
    console.error(e);
    throw new Error(`Error while storing car to IPFS for stream: ${e}`);
  }
}

async function getAnchorStatus(CommitID: string): Promise<any> {
  const casUrl = `${CAS_URL}/api/v0/requests/${CommitID}`;
  const digest = await createAuthHeader(casUrl, CommitID);
  try {
    console.log(`Fetching anchor status for stream ${CommitID}`);
     const response = await axios.get(casUrl, { 
      headers: {
        Authorization: digest,
        'Content-Type': 'application/json',
      },  
    });
    return response.data;
  } catch (error) {
    console.error(`Failure: Error fetching anchor status for stream ${CommitID}:`, error.message);
    return null;
  }
}


function decodeWitnessCAR(witnessCAR) {
  const base64 = witnessCAR.replace(/-/g, '+').replace(/_/g, '/');
  const padded = base64.padEnd(base64.length + (4 - base64.length % 4) % 4, '=');
  const binaryString = atob(padded);
  return Uint8Array.from(binaryString, char => char.charCodeAt(0));
}

// Parsing witnessCAR and converting it to a standard CAR
async function parseWitnessCarFile(witnessCar: CAR){
  try {
    const decodedData = await decodeWitnessCAR(witnessCar);
    await uploadToIPFS(decodedData);
    return true;
  } catch (error) {
    console.error('Error: Parsing error', error);
    return null;
  }
}

async function processStreams(filePath: string): Promise<void> {
  const streams: StreamData[] = [];
  console.log("TEST 1");
  fs.createReadStream(filePath)
    .pipe(csv())
    .on('data', (data: StreamData) => streams.push(data))
    .on('end', async () => {
      for (const stream of streams) {
        console.log(`Processing: commit ${stream["Commit ID"]}`);
        const anchorStatus = await getAnchorStatus(stream["Commit ID"]);
        if (anchorStatus) {
          if (anchorStatus.status === 'COMPLETED' && anchorStatus.witnessCar) {
            const carFileStatus = await parseWitnessCarFile(anchorStatus.witnessCar);
            if (carFileStatus) {
              const streamID = StreamID.fromString(anchorStatus.streamId);
              const anchorCommitID = CommitID.make(streamID, anchorStatus.anchorCommit.cid);
              const ceramic = new CeramicClient("http://localhost:7007");
              try {
                const stream = await ceramic.loadStream(anchorCommitID);
                if(stream) {
                  console.log(`Success: Stream ${stream.id.toString()} loaded successfully`);
                }
              }
              catch (error) {
                console.log(`StreamFailure: failed to load teh stream ${anchorCommitID}:`, error.message);
              }
            }
          }
          else{
            console.log(`FAIL: Anchor status is not completed for commit: ${stream["Commit ID"]}`);
          }
        } else {
          console.log(`FAIL: No anchor status for commit : ${stream["Commit ID"]}`);
        }
      }
    });
}


const csvFilePath = 'streams_all.csv';
processStreams(csvFilePath);
