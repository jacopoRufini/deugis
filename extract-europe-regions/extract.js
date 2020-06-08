const fs = require('fs');
const request = require('request');
const jsdom = require("jsdom");
const { JSDOM } = jsdom;
const DEFAULT_URL = 'http://download.geofabrik.de/'
const START_URL = DEFAULT_URL + 'europe.html'
const filename = "urls.txt"

//delete old file if exists
if (fs.existsSync(filename)) {
    fs.unlinkSync(filename)
}

visit(START_URL)

/**
 * Visits recursively http://download.geofabrik.de/ to extract all the urls of europe regions
 * @param {String} url to be visited
 */
function visit(url) {
    if (!url) return
    request(url, function (err, res, body) {
        const document = new JSDOM(body).window.document
        removeDetailsAndSpecialSubregions(document)
        const subregions = document.getElementsByClassName('subregion')
        if (subregions.length > 0) {
            for (let region of subregions) {
                const nextUrl = getNextUrl(url, region)
                visit(nextUrl)
            }
        } else {
            const link = extractLink(url, document)
            fs.appendFileSync(filename, link + "\n")
        }
    });
}

/**
 * Returns the next url to visit 
 * @param {String} url to be visited
 * @param {Document} region that links to the url
 */
const getNextUrl = (url, region) => {
    const a = region.getElementsByTagName('a')[0]
    if (a) {
        return getPath(url) + a.href
    }
}

/**
 * Returns the download link of the selected region
 * @param {String} url to the download link of the region
 * @param {Document} document of the html page
 */
const extractLink = (url, document) => {
    try {
        const ul = document.getElementsByTagName('ul')[0]
        return getPath(url) + ul.getElementsByTagName('a')[0].href
    } catch (ex) {
        return null
    }
}

/**
 * Returns the url without the ending part of path
 * @param {String} url 
 */
const getPath = (url) => {
    return url.match(".+/")[0]
}

/**
 * Removes a useless node that shares same id of subregions
 * @param {Document} document of the html file
 */
const removeDetailsAndSpecialSubregions = (document) => {
    const details = document.getElementById('details')
    if (details) details.remove()
    const subregions = document.getElementById('specialsubregions')
    if (subregions) subregions.remove()
}