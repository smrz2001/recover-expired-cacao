import fs from "fs";
import csv from "csv-parser";
import axios from "axios";
import {v4 as uuidv4} from 'uuid';
import {DID} from 'dids';
import {Ed25519Provider} from 'key-did-provider-ed25519';
import {getResolver} from 'key-did-resolver';
import {fromString} from 'uint8arrays';
import {CommitID, StreamID} from '@ceramicnetwork/streamid';
import {CeramicClient} from "@ceramicnetwork/http-client";

const CAS_URL = "https://cas.3boxlabs.com";
const CERAMIC_URL = "http://localhost:7007";

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
        console.error(`Failure: Error fetching anchor status for stream ${CommitID}:`, error.message);
        return null;
    }
}

async function repairStreams(filePath: string): Promise<void> {
    const streams: StreamData[] = [];
    fs.createReadStream(filePath)
        .pipe(csv())
        .on('data', (data: StreamData) => streams.push(data))
        .on('end', async () => {
            for (const stream of streams) {
                console.log(`Processing: commit ${stream["Commit ID"]}`);
                const anchorStatus = await getAnchorStatus(stream["Commit ID"]);
                if (anchorStatus) {
                    if (anchorStatus.status === 'COMPLETED' && anchorStatus.witnessCar) {
                        const streamID = StreamID.fromString(anchorStatus.streamId);
                        const anchorCommitID = CommitID.make(streamID, anchorStatus.anchorCommit.cid);
                        const ceramic = new CeramicClient(CERAMIC_URL);
                        try {
                            const stream = await ceramic.loadStream(anchorCommitID);
                            if (stream) {
                                console.log(`Success: Stream ${stream.id.toString()} loaded successfully`);
                            }
                        } catch (error) {
                            // @ts-ignore
                            console.log(`StreamFailure: failed to load the stream ${anchorCommitID}:`, error.message);
                        }
                    }
                } else {
                    // @ts-ignore
                    console.log(`FAIL: Anchor status is not completed for commit: ${stream["Commit ID"]}`);
                }
            }
        });
}

const csvFilePath = 'streams_all.csv';
repairStreams(csvFilePath);
