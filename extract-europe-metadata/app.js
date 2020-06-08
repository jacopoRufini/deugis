const fs = require('fs')
const request = require('request')
const url = "https://nominatim.openstreetmap.org/search.php?q="
const utf8 = require('utf8');

let data
let list = require('./list.js')

process.on('SIGINT', function() {
    saveAndExit(data)
});

if (!fs.existsSync("data.json")) {
    fs.writeFileSync('data.json', '', 'utf8')
}

try {
    data = JSON.parse(fs.readFileSync('data.json'))
    list = list.slice(data.length,list.length+1)
} catch(e) {
    data = []
}

const id = setInterval(() => {
    if (list.length > 0) {
        const name = list.shift()

        var options = {
            url: url + escape(utf8.encode(name)) + "&format=json",
            headers: {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X x.y; rv:42.0) Gecko/20100101 Firefox/42.0'
            },
            encoding: 'UTF-8'
        };
        console.log(options['url'])
        request(options, (err,res,body) => {
            if (err) console.log(err)
            try {
                if (res.statusCode === 200) {
                    try {
                        data.push([name, JSON.parse(body)[0]['osm_id']])
                        console.log("downloaded " + name + ". Missing: " + list.length)
                    }
                    catch (e) {
                        console.log("\nError on " + name)
                        console.log(e)
                        saveAndExit(data)
                    }
                } else {
                    console.log(body)
                    saveAndExit(data)
                }
            } catch(e) {
                console.log(e)
            }
        })
    } else {
        saveAndExit(data)
        clearInterval(id)
    }
}, (0.6 * 1000 ))

const saveAndExit = (data) => {
    fs.writeFileSync("data.json", JSON.stringify(data), 'utf8')
    process.exit()
}