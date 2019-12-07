const fs = require('fs');
const { resolve } = require('path');
const { promisify } = require('util');
const child_process = require('child_process');
const { fromFile: hashFromFile } = require('hasha');
const fetch = require('node-fetch');
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

/**
 * Simple wrapper for fetch that implements an exponental backoff on network error.
 *
 * @param {string|URL} input
 * @param {object} options
 * @param {int} iteration
 */
async function backoffFetch(input, options, iteration = 0) {
    try {
        const response = await fetch(input, options);

        if (!response.ok) {
            const seconds = (2 ** iteration);
            console.log(`Request Error: ${response.status} ${repsonse.statusText}`);
            console.log(`Retrying in ${seconds.toLocaleString()} seconds`);
            await timeout(seconds * 1000);
            return backoffFetch(input, options, iteration + 1);
        }

        const data = await response.json();

        return data;
    } catch (e) {
        const seconds = (2 ** iteration);
        console.log(`Request Error: ${e.message}`);
        console.log(`Retrying in ${seconds.toLocaleString()} seconds`);
        await timeout(seconds * 1000);
        return backoffFetch(input, options, iteration + 1);
    }
}

/**
 * Wrapper for cookieFetch that handles exponental backoff on network failure, and throttled retry.
 *
 * @param {string|URL} input
 * @param {object} options
 * @param {int} iteration
 */
async function retryFetch(input, options, iteration = 0) {
    try {
        const response = await cookieFetch(input, options);

        if (!response.ok) {
            const seconds = (2 ** iteration);
            console.log(`Request Error: ${response.status} ${repsonse.statusText}`);

            if (response.status >= 500) {
                console.log(`Retrying in ${seconds.toLocaleString()} seconds`);
                await timeout(seconds * 1000);
                return retryFetch(input, options, iteration + 1);
            }
        }

        const data = await response.json();

        if ( data.error ) {
            if ( data.error.messages ) {
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
            } else {
                console.error(data.error);
                return data;
            }
        }

        return data;
    } catch (e) {
        const seconds = (2 ** iteration);
        console.log(`Request Error: ${e.message}`);
        console.log(`Retrying in ${seconds.toLocaleString()} seconds`);
        await timeout(seconds * 1000);
        return retryFetch(input, options, iteration + 1);
    }
}

async function getToken() {
    const url = new URL('https://www.wikidata.org/w/api.php');
    url.searchParams.set('action', 'query');
    url.searchParams.set('format', 'json');
    url.searchParams.set('formatversion', 2);
    url.searchParams.set('meta', 'tokens');

    const data = await retryFetch(url);

    return data.query.tokens.csrftoken;
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
    console.log(`Retreiving contributions`);
    while (cont !== false) {
        if ( cont ) {
            console.log(`Retreiving contributions ${cont}`);
            userContribsUrl.searchParams.set('uccontinue', cont);
        }
        const userContribsResponse = await cookieFetch(userContribsUrl);
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

    console.log(`Number of items edited previously: ${edited.size.toLocaleString()}`);

    const dataFolder = resolve(__dirname, 'data');
    try {
        await mkdir(dataFolder);
    } catch (e) {
        if (e.code !== 'EEXIST') {
            throw e;
        }
    }
    const db = await open(resolve(dataFolder, 'data.sqlite'));

    const createMapTable = db.run(`
        CREATE TABLE IF NOT EXISTS map (
            wd TEXT NOT NULL,
            wof INTEGER NOT NULL,
            PRIMARY KEY (wd, wof)
        );
    `);
    const otherName = 'woeid';
    const createOtherTable = db.run(`
        CREATE TABLE IF NOT EXISTS ${otherName} (
            id INTEGER NOT NULL,
            PRIMARY KEY (id)
        );
    `);

    // Loop through and get the Who's on First id for each item previously edited.
    const wofIds = new Set();

    // Wait for the table creation to finish.
    await createMapTable;
    const result = await db.all('SELECT wd, wof FROM map');

    const missingEntity = new Set(edited);

    if ( result.length > 0 ) {
        result.forEach( ( row ) => {
            // Remove items we already know about from the list of what needs updating
            if ( missingEntity.has( row.wd ) ) {
                missingEntity.delete( row.wd );
            }

            wofIds.add( parseInt(row.wof, 10) );
        } );
    }

    const wofProperty = 'P6766';

    for ( const entity of missingEntity ) {
        console.log(`Retrieving Who's on First ID for ${entity}`);

        const wofUrl = new URL('https://www.wikidata.org/w/api.php');
        wofUrl.searchParams.set('action', 'wbgetclaims');
        wofUrl.searchParams.set('format', 'json');
        wofUrl.searchParams.set('formatversion', 2);
        wofUrl.searchParams.set('entity', entity);
        wofUrl.searchParams.set('property', wofProperty);

        const wofData = await retryFetch(wofUrl);

        if (
            wofData
            && wofData.claims
            && wofData.claims[wofProperty]
            && Array.isArray(wofData.claims[wofProperty])
            && wofData.claims[wofProperty].length > 0
        ) {
            for ( const claim of wofData.claims[wofProperty] ) {
                if (
                    claim.mainsnak
                    && claim.mainsnak.datavalue
                    && claim.mainsnak.datavalue.value
                ) {
                    const wofId = parseInt(claim.mainsnak.datavalue.value, 10);
                    await db.run(`INSERT INTO map VALUES (?, ?)`, entity, wofId );
                    wofIds.add( wofId );
                }
            }
        }
    }

    let list = [];

    // Query the number of wikidata items for each database.
    for ( const file of files ) {
        console.log(`Querying ${file.name} start`);

        const wofDB = await open(resolve(downloadsFolder, file.name));
        const otherSource = 'gp:id';
        const result = await wofDB.all(`
            SELECT
                spr.id,
                c.other_id
            FROM spr
            INNER JOIN concordances AS c ON spr.id = c.id
            WHERE
                spr.is_current != 0
                AND c.other_source = ?
                AND c.other_id NOT IN (-99, -1, 0)
            GROUP BY spr.id
        `, otherSource);

        if (result.length > 0) {
            list = [
                ...list,
                ...result,
            ];
        }

        console.log(`Querying ${file.name} end`);
    }

    // Remove any duplicates
    const items = list.reduce(( map, item ) => {
        map.set( parseInt( item.id, 10 ), item);
        return map;
    }, new Map());

    // Remove items that already have a Wikidata item
    for ( let wofId of wofIds ) {
        if ( items.has( wofId ) ) {
            items.delete( wofId );
        }
    }

    // Wait for the table creation to finish.
    await createOtherTable;
    // Get all of the existing ids that are known to be missing.
    const otherRows = await db.all(`SELECT id FROM ${otherName}`);
    const others = new Set(otherRows.map(row => parseInt(row.id, 10)));

    // Remove any items that are known to not exist.
    for ( const [key, { other_id }] of items.entries() ) {
        if (others.has(parseInt(other_id, 10))) {
            items.delete(key);
        }
    }

    console.log(`Number of Wikidata items to edit: ${items.size.toLocaleString()}`);

    const otherProperty = 'P1281';

    let i = 0;
    // Edit Wikidata, one at a time.
    for ( const { id, other_id } of items.values() ) {
        i++;
        const percent = Math.floor((i / items.size) * 100);
        // Search for the item
        const searchUrl = new URL('https://www.wikidata.org/w/api.php');
        searchUrl.searchParams.set('action', 'query');
        searchUrl.searchParams.set('format', 'json');
        searchUrl.searchParams.set('formatversion', 2);
        searchUrl.searchParams.set('list', 'search');
        // Search for the other property, but exclude items that already have a Who's on First ID.
        searchUrl.searchParams.set('srsearch', `haswbstatement:${otherProperty}=${other_id} -haswbstatement:${wofProperty}`);
        searchUrl.searchParams.set('srlimit', 1);
        searchUrl.searchParams.set('srinfo', '');
        searchUrl.searchParams.set('srprop', '');

        const searchData = await backoffFetch(searchUrl);

        if (typeof searchData.query === 'undefined' || typeof searchData.query.search === 'undefined' || searchData.query.search.length === 0 ) {
            console.log(`(${percent}%) Skipping ${id} No Entity Found`);
            // Don't wait for the SQL query to finish before proceeding to the next value.
            db.run(`INSERT OR IGNORE INTO ${otherName} VALUES (?)`, other_id);
            continue;
        }

        const entityId = searchData.query.search[0].title;

        if ( edited.has( entityId ) ) {
            console.log(`(${percent}%) Skipping ${entityId} Already Edited`);
            continue;
        }

        console.log(`(${percent}%) Editing ${entityId} start`);

        const token = await getToken();

        const editUrl = new URL('https://www.wikidata.org/w/api.php');
        const editFormData = new URLSearchParams();
        editFormData.set('action', 'wbcreateclaim');
        editFormData.set('format', 'json');
        editFormData.set('formatversion', 2);
        editFormData.set('entity', entityId);
        editFormData.set('snaktype', 'value');
        editFormData.set('property', wofProperty);
        // Must be surrounded by quotes!
        editFormData.set('value', `"${id}"`);
        editFormData.set('token', token);
        editFormData.set('bot', 1);

        await retryFetch(editUrl, {
            method: 'POST',
            body: editFormData,
        });

        console.log(`(${percent}%) Editing ${entityId} end`);

    }
}

main();
