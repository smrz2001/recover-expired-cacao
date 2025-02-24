import fs from "fs";
import csv from "csv-parser";
import axios from "axios";
import {v4 as uuidv4} from 'uuid';
import {DID} from 'dids';
import {Ed25519Provider} from 'key-did-provider-ed25519';
import {getResolver} from 'key-did-resolver';
import {fromString} from 'uint8arrays';
import {type CAR} from 'cartonne'
import {writeFile} from 'fs/promises';

const CAS_URL = "https://cas.3boxlabs.com";

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

    const nodePrivateKey = process.env["NODE_PRIVATE_KEY"];
    if (!nodePrivateKey) {
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
    // @ts-ignore
    const protectedHeader = jws.signatures[0].protected;
    // @ts-ignore
    const signature = jws.signatures[0].signature;
    return `Bearer ${protectedHeader}.${jws.payload}.${signature}`;
}

interface StreamData {
    streamId: string;
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
        // @ts-ignore
        console.error(`Failure: Error fetching anchor status for stream ${CommitID}:`, error.message);
        return null;
    }
}


function decodeWitnessCAR(witnessCAR: CAR) {
    const base64 = witnessCAR.toString().replace(/-/g, '+').replace(/_/g, '/');
    const padded = base64.padEnd(base64.length + (4 - base64.length % 4) % 4, '=');
    const binaryString = atob(padded);
    return Uint8Array.from(binaryString, char => char.charCodeAt(0));
}

async function writeUint8ArrayToFile(uint8Array: Uint8Array, filePath: string) {
    try {
        await writeFile(filePath, uint8Array);
        return true; // Success
    } catch (error) {
        if (error instanceof Error) {
            console.error(`Failed to write file: ${error.message}`);
        } else {
            console.error('An unknown error occurred while writing the file');
        }
        throw error; // Re-throw to allow caller to handle
    }
}

// Parse and store the witness CAR file
async function storeWitnessCarFile(commitID: any, witnessCar: CAR) {
    try {
        const decodedData = decodeWitnessCAR(witnessCar);
        // Write the file to disk using its commit ID
        await writeUint8ArrayToFile(decodedData, `./cars/${commitID}`);
        return true;
    } catch (error) {
        console.error('Error: Parsing error', error);
        return null;
    }
}

async function fetchCars(filePath: string): Promise<void> {
    const streams: StreamData[] = [];
    fs.createReadStream(filePath)
        .pipe(csv())
        .on('data', (data: StreamData) => streams.push(data))
        .on('end', async () => {
            for (const stream of streams) {
                // @ts-ignore
                const commitID = stream["Commit ID"];
                console.log(`Processing: commit ${commitID}`);
                const anchorStatus = await getAnchorStatus(commitID);
                if (anchorStatus) {
                    if (anchorStatus.status === 'COMPLETED' && anchorStatus.witnessCar) {
                        await storeWitnessCarFile(commitID, anchorStatus.witnessCar);
                    } else {
                        console.log(`FAIL: Anchor status is not completed for commit: ${commitID}`);
                    }
                } else {
                    console.log(`FAIL: No anchor status for commit : ${commitID}`);
                }
            }
        });
}


const csvFilePath = 'streams_all.csv';
fetchCars(csvFilePath);
