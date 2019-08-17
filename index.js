const fs = require('fs');
const { resolve } = require('path');
const { promisify } = require('util');
const child_process = require('child_process');
const { fromFile: hashFromFile } = require('hasha');
const fetch = require('node-fetch');
const { remove } = require('fs-extra');
const { DateTime } = require('luxon');
const promisePipe = require('promisepipe');
const SQL = require('sql-template-strings');
const { open } = require('sqlite');
const FormData = require('form-data');
const cookieFetch = require('fetch-cookie')(fetch);

const mkdir = promisify(fs.mkdir);
const unlink = promisify(fs.unlink);
const exec = promisify(child_process.exec);

function timeout(ms) {
    return new Promise(resolve => setTimeout(resolve, ms))
}

async function retryFetch(input, options) {
    const response = await cookieFetch(input, options);
    const data = await response.json();

    if ( data.error && data.error.messages ) {
        if (data.error.messages.find(({name}) => name === 'actionthrottledtext')) {
            console.log(`Request Throttled, Retrying in 60 seconds`);
            await timeout(1000 * 60);
            return retryFetch(input, options)
        } else {
            console.error(data.error);
            return data;
        }
    }

    return data;
}

async function main() {
    const response = await fetch('https://dist.whosonfirst.org/sqlite/inventory.json');
    const data = await response.json();

    const files = [ ...data.reduce((map, item) => {
        if (!map.has(item.repo)) {
            map.set(item.repo, item);
            return map;
        }

        const existing = map.get(item.repo);
        if (DateTime.fromISO(existing.last_modified) < DateTime.fromISO(item.last_modified)) {
            map.set(item.repo, item);
            return map;
        }

        return map;
    }, new Map()).values() ];

    const downloadsFolder = resolve(__dirname, 'downloads');

    try {
        await mkdir(downloadsFolder);
    } catch (e) {
        if (e.code !== 'EEXIST') {
            throw e;
        }
    }

    let downloadList = [];

    // Check the file hash, one at a time.
    for ( file of files ) {
        console.log(`Checking Hash ${file.name_compressed} start`);

        let hash;
        try {
            hash = await hashFromFile(resolve(downloadsFolder, file.name_compressed), { algorithm: 'sha256' });
        } catch (e) {
            if (e.code !== 'ENOENT') {
                throw e;
            }
        }

        if (hash !== file.sha256_compressed) {
            downloadList = [
                ...downloadList,
                file,
            ];
        }

        console.log(`Checking Hash ${file.name_compressed} end`);
    }

    console.log(`Downloading ${downloadList.length} files`);

    // Download each file, one at a time.
    for ( file of downloadList ) {
        console.log(`Download ${file.name_compressed} start`);
        try {
            await unlink(resolve(downloadsFolder, file.name_compressed));
        } catch(e) {
            if (e.code !== 'ENOENT') {
                throw e;
            }
        }
        const download = await fetch(`https://dist.whosonfirst.org/sqlite/${file.name_compressed}`);
        await promisePipe(download.body, fs.createWriteStream(resolve(downloadsFolder, file.name_compressed)));
        console.log(`Download ${file.name_compressed} end`);

        console.log(`Decompressing ${file.name_compressed} start`);
        try {
            await unlink(resolve(downloadsFolder, file.name));
        } catch(e) {
            if (e.code !== 'ENOENT') {
                throw e;
            }
        }
        await exec(`bunzip2 -k ${resolve(downloadsFolder, file.name_compressed)}`);
        console.log(`Decompressing ${file.name_compressed} end`);
    }

    let list = [];

    // Query the number of wikidata items for each database.
    for ( file of files ) {
        const db = await open(resolve(downloadsFolder, file.name));
        const result = await db.all("SELECT id, other_id FROM concordances WHERE other_source = 'wd:id'");

        if (result.length > 0) {
            list = [
                ...list,
                ...result,
            ];
        }
    }

    console.log(`Number of Wikidata items: ${list.length.toLocaleString()}`);

    const loginTokenUrl = new URL('https://www.wikidata.org/w/api.php');
    loginTokenUrl.searchParams.set('action', 'query');
    loginTokenUrl.searchParams.set('format', 'json');
    loginTokenUrl.searchParams.set('formatversion', 2);
    loginTokenUrl.searchParams.set('meta', 'tokens');
    loginTokenUrl.searchParams.set('type', 'login');

    const loginTokenResponse = await cookieFetch(loginTokenUrl);
    const loginTokenData = await loginTokenResponse.json();

    const { logintoken } = loginTokenData.query.tokens;

    const loginUrl = new URL('https://www.wikidata.org/w/api.php');

    const loginFormData = new FormData();
    loginFormData.append('action', 'login');
    loginFormData.append('format', 'json');
    loginFormData.append('formatversion', 2);
    loginFormData.append('lgname', 'Q23679');
    loginFormData.append('lgpassword', process.env.PASSWORD);
    loginFormData.append('lgtoken', logintoken);

    const loginResponse = await cookieFetch(loginUrl, {
        method: 'POST',
        body: loginFormData,
    });
    const loginData = await loginResponse.json();

    console.log(`Login Result: ${loginData.login.result}`);

    const csrfTokenUrl = new URL('https://www.wikidata.org/w/api.php');
    csrfTokenUrl.searchParams.set('action', 'query');
    csrfTokenUrl.searchParams.set('format', 'json');
    csrfTokenUrl.searchParams.set('formatversion', 2);
    csrfTokenUrl.searchParams.set('meta', 'tokens');

    const csrfTokenResponse = await cookieFetch(csrfTokenUrl);
    const csrfTokenData = await csrfTokenResponse.json();

    const { csrftoken } = csrfTokenData.query.tokens;

    const property = 'P6766';

    // Edit Wikidata, one at a time.
    for ( { id, other_id } of list ) {
        const url = new URL('https://www.wikidata.org/w/api.php');
        url.searchParams.set('action', 'wbgetclaims');
        url.searchParams.set('format', 'json');
        url.searchParams.set('formatversion', 2);
        url.searchParams.set('property', property);
        url.searchParams.set('entity', other_id);

        const response = await fetch(url);
        const { claims } = await response.json();

        if ( !claims[property] ) {
            console.log(`Editing ${other_id} start`);
            const editUrl = new URL('https://www.wikidata.org/w/api.php');
            const editFormData = new FormData();
            editFormData.append('action', 'wbcreateclaim');
            editFormData.append('format', 'json');
            editFormData.append('formatversion', 2);
            editFormData.append('entity', other_id);
            editFormData.append('snaktype', 'value');
            editFormData.append('property', property);
            // Must be surrounded by quotes!
            editFormData.append('value', `"${id}"`);
            editFormData.append('token', csrftoken);
            editFormData.append('bot', 1);

            await retryFetch(editUrl, {
                method: 'POST',
                body: editFormData,
            });

            console.log(`Editing ${other_id} end`);
        } else {
            console.log(`Skipping ${other_id}`);
        }

    }
}

main();
