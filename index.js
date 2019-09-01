const fs = require('fs');
const { resolve } = require('path');
const { promisify } = require('util');
const child_process = require('child_process');
const { fromFile: hashFromFile } = require('hasha');
const fetch = require('node-fetch');
const { remove } = require('fs-extra');
const { DateTime } = require('luxon');
const promisePipe = require('promisepipe');
const { open } = require('sqlite');
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
        } else if (data.error.messages.find(({name}) => name === 'no-permission')) {
            console.error('Permission Denied!');
            return data;
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

    const loginFormData = new URLSearchParams();
    loginFormData.set('action', 'login');
    loginFormData.set('format', 'json');
    loginFormData.set('formatversion', 2);
    loginFormData.set('lgname', 'Q23679');
    loginFormData.set('lgpassword', process.env.PASSWORD);
    loginFormData.set('lgtoken', logintoken);

    const loginResponse = await cookieFetch(loginUrl, {
        method: 'POST',
        body: loginFormData,
    });
    const loginData = await loginResponse.json();

    console.log(`Login Result: ${loginData.login.result}`);

    if ( loginData.login.result !== 'Success' ) {
        throw new Error('Login Failure');
    }

    const userContribsUrl = new URL('https://www.wikidata.org/w/api.php');
    userContribsUrl.searchParams.set('action', 'query');
    userContribsUrl.searchParams.set('format', 'json');
    userContribsUrl.searchParams.set('formatversion', 2);
    userContribsUrl.searchParams.set('list', 'usercontribs');
    userContribsUrl.searchParams.set('uclimit', 'max');
    userContribsUrl.searchParams.set('ucnamespace', 0);
    userContribsUrl.searchParams.set('ucuser', 'Q23679');
    userContribsUrl.searchParams.set('ucprop', 'title');

    let cont = null;
    let edited = new Set();
    while (cont !== false) {
        if ( cont ) {
            userContribsUrl.searchParams.set('uccontinue', cont);
        }
        const userContribsResponse = await cookieFetch(userContribsUrl, {
            method: 'POST',
            body: loginFormData,
        });
        const userContribsData = await userContribsResponse.json();

        // Set the continue.
        if ( userContribsData.continue && userContribsData.continue.uccontinue ) {
            cont = userContribsData.continue.uccontinue;
        } else {
            cont = false;
        }

        // Add each titles.
        if ( userContribsData.query && userContribsData.query.usercontribs ) {
            userContribsData.query.usercontribs.forEach(({ title }) => edited.add(title));
        }
    }

    console.log(`Number of items edited previously: ${edited.size.toLocaleString()}`)

    let list = [];

    // Query the number of wikidata items for each database.
    for ( file of files ) {
        const db = await open(resolve(downloadsFolder, file.name));
        const result = await db.all("SELECT c.id, c.other_id FROM concordances AS c JOIN spr ON c.id = spr.id WHERE c.other_source = 'wd:id' AND spr.is_deprecated != 1 GROUP BY spr.id");

        if (result.length > 0) {
            list = [
                ...list,
                ...result.filter(({ other_id }) => !edited.has( other_id )),
            ];
        }
    }

    console.log(`Number of Wikidata items to edit: ${list.length.toLocaleString()}`);

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
        const data = await response.json();
        const { claims } = data;

        if (typeof claims === 'undefined') {
            console.error(data);
        } else if ( !claims[property] ) {
            console.log(`Editing ${other_id} start`);
            const editUrl = new URL('https://www.wikidata.org/w/api.php');
            const editFormData = new URLSearchParams();
            editFormData.set('action', 'wbcreateclaim');
            editFormData.set('format', 'json');
            editFormData.set('formatversion', 2);
            editFormData.set('entity', other_id);
            editFormData.set('snaktype', 'value');
            editFormData.set('property', property);
            // Must be surrounded by quotes!
            editFormData.set('value', `"${id}"`);
            editFormData.set('token', csrftoken);
            editFormData.set('bot', 1);

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
