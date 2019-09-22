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

const placetypes = new Map(Object.entries({
    address: 'Q319608',
    arcade: 'Q11315',
    borough: 'Q5195043',
    building: 'Q41176',
    campus: 'Q209465',
    concourse: 'Q862212',
    constituency: 'Q192611',
    continent: 'Q5107',
    country: 'Q6256',
    county: 'Q28575',
    dependency: 'Q161243',
    disputed: 'Q15239622',
    empire: 'Q48349',
    enclosure: 'Q5375483',
    installation: 'Q20437094',
    intersection: 'Q285783',
    localadmin: 'Q66941850',
    locality: 'Q486972',
    macrocounty: 'Q66980082',
    macrohood: 'Q66980180',
    macroregion: 'Q3434769',
    marinearea: 'Q66980635',
    marketarea: 'Q6770790',
    metroarea: 'Q1907114',
    microhood: 'Q66980952',
    neighbourhood: 'Q123705',
    ocean: 'Q9430',
    planet: 'Q634',
    postalcode: 'Q37447',
    region: 'Q3455524',
    timezone: 'Q12143',
    venue: 'Q17350442',
    wing: 'Q1125776',
}));

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

    for ([type, id] of placetypes ) {
        const query = `SELECT ?item
        WHERE {
            ?item wdt:P279* wd:${id}.
        }`;
        const queryUrl = new URL('https://query.wikidata.org/sparql');
        queryUrl.searchParams.set('query', query);
        queryUrl.searchParams.set('format', 'json');

        const result = await fetch(queryUrl);
        const data = await result.json();

        const ids = data.results.bindings.map(({ item }) => {
            const uri = new URL(item.value);

            return uri.pathname.split('/').pop();
        });

        // Set the placetype implementations.
        console.log(`Class ${id} is subclassed by ${ids.length.toLocaleString()} items`)
        placetypes.set(type, new Set(ids));
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

    console.log(`Number of items edited previously: ${edited.size.toLocaleString()}`)

    let list = [];

    // Query the number of wikidata items for each database.
    for ( file of files ) {
        console.log(`Querying ${file.name} start`);

        const db = await open(resolve(downloadsFolder, file.name));
        const result = await db.all(`
            SELECT
                spr.id,
                spr.placetype,
                c.other_id
            FROM spr
            INNER JOIN concordances AS c ON spr.id = c.id
            WHERE
                spr.is_deprecated != 1
                AND c.other_source = 'gn:id'
                AND c.other_id NOT IN (-99, -1, 0)
            GROUP BY spr.id
            HAVING NOT c.other_source = 'wd:id'
        `);

        if (result.length > 0) {
            list = [
                ...list,
                ...result,
            ];
        }

        console.log(`Querying ${file.name} end`);
    }

    console.log(`Number of Wikidata items to edit: ${list.length.toLocaleString()}`);

    const otherProperty = 'P1566';
    const wofProperty = 'P6766';
    const instanceProperty = 'P31';

    // Edit Wikidata, one at a time.
    for ( { id, placetype, other_id } of list ) {
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
            console.log(`Skipping ${id} No Entity Found`);
            continue;
        }

        const entityId = searchData.query.search[0].title;

        if ( edited.has( entityId ) ) {
            console.log(`Skipping ${entityId} Already Edited`);
            continue;
        }

        const instanceUrl = new URL('https://www.wikidata.org/w/api.php');
        instanceUrl.searchParams.set('action', 'wbgetclaims');
        instanceUrl.searchParams.set('format', 'json');
        instanceUrl.searchParams.set('formatversion', 2);
        instanceUrl.searchParams.set('property', instanceProperty);
        instanceUrl.searchParams.set('entity', entityId);

        const instanceData = await backoffFetch(instanceUrl);

        if (typeof instanceData.claims === 'undefined') {
            console.log(`Skipping ${entityId} Error`);
            console.error(instanceData);
            continue;
        }

        if (instanceData.claims[instanceProperty]) {
            const instanceOf = instanceData.claims[instanceProperty].map(claim => claim.mainsnak.datavalue.value.id);

            if (instanceOf.length > 0) {
                const isValidInstance = instanceOf.find(id => placetypes.get(placetype).has(id));

                if (!isValidInstance) {
                    console.log(`Skipping ${entityId} with ${instanceProperty} of ${instanceOf.join(', ')}`);
                    continue;
                }
            }
        }

        console.log(`Editing ${entityId} start`);

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

        console.log(`Editing ${entityId} end`);

    }
}

main();
