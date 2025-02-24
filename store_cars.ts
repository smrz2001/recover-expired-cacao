import fs from "fs";
import {CAR, CARFactory } from 'cartonne'
import {CeramicClient} from '@ceramic-sdk/http-client'

const C1_URL = "http://ceramic-one-0:5101";

const cf = new CARFactory();

async function uploadToC1(car: CAR, ceramicOneClient: CeramicClient): Promise<void> {
    try {
        await ceramicOneClient.postEventCAR(car);
    } catch (e) {
        console.error(e);
        throw new Error(`Error while storing car to C1 for stream: ${e}`);
    }
}

async function storeCars(filePath: string): Promise<void> {
    const files = fs.readdirSync(filePath);
    const ceramicOneClient = new CeramicClient({url: C1_URL});
    for (const file of files) {
        const carBytes = fs.readFileSync(`${filePath}/${file}`);
        const car = cf.fromBytes(carBytes);
        try {
            await uploadToC1(car, ceramicOneClient);
            console.log(`Stored car ${car.roots[0]}`);
        } catch (e) {
            console.error(e);
        }
    }
}

const carsPath = './cars';
storeCars(carsPath);
