mapboxgl.accessToken = 'pk.eyJ1IjoiamFjb3BvcnVmaW5pIiwiYSI6ImNrNDN5ZjMweTBjMWgzbHJ6cm50N2lnczkifQ.uPkI55fjO5rmYYYsEWufrw';

var map = loadMap("streets-v11", [15.43646, 47.78399], 4, 70);

function loadMap(mapName, center, zoom, pitch) {
    clearMap();
    return new mapboxgl.Map({
        container: 'map',
        style: 'mapbox://styles/mapbox/' + mapName,
        center: center,
        pitch: pitch, // pitch in degrees
        minZoom: 3,
        zoom: zoom
    });
}

/*
AGGIUNGERE DOPO
map.on('load', function() {
    map.addSource('10m-bathymetry-81bsvj', {
        type: 'vector',
        url: 'mapbox://mapbox.9tm8dx88'
    });

    map.addLayer(
        {
            'id': '10m-bathymetry-81bsvj',
            'type': 'fill',
            'source': '10m-bathymetry-81bsvj',
            'source-layer': '10m-bathymetry-81bsvj',
            'layout': {},
            'paint': {
                'fill-outline-color': 'hsla(337, 82%, 62%, 0)',
                'fill-color': [
                    'interpolate',
                    ['cubic-bezier', 0, 0.5, 1, 0.5],
                    ['get', 'DEPTH'],
                    200,
                    '#78bced',
                    9000,
                    '#15659f'
                ]
            }
        },
        'land-structure-polygon'
    );
});
 */

const heatAnalysis = document.getElementById("heatAnalysis");
const interpolation = document.getElementById("interpolation");

interpolation.onclick = () => {
    map = loadMap("streets-v11", [8.41543, 45.32298], 8, 0);
    map.on('load', ()=>{ showInterpolation() })
};

heatAnalysis.onclick = () => {
    map = loadMap('dark-v10', [15.43646, 47.78399], 4, 70);
    map.on('load', ()=>{ showHeatMap() })
};

function clearMap() {
    if (map != null) document.getElementById('map').innerHTML = ''
}

function getRandomID() {
    return Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15)
}


//from here function that may be used in future


/*
wktButton.onclick = function () {
    const lineString = [];
    const polygons = [];
    const wkt = document.getElementById('wktValue').value;
    const arrayWKT = (JSON.parse(wkt)["wkt"]);
    arrayWKT.forEach(elm => {
        const primitive = Terraformer.WKT.parse(elm);
        if (primitive.type === "LineString") lineString.push(primitive);
    });
    console.log(buildLines(lineString));
    map.addLayer(buildLines(lineString));

};

 */
function buildLine(obj) {
    return {
        'type': 'Feature',
        'properties': {
            'color': '#f700cd'
        },
        'geometry': {
            'type': 'LineString',
            'coordinates': obj.coordinates
        }
    }
}

function buildLines(arrayPrimitives) {
    const linesOBJ = {
        'id': getRandomID(),
        'type': 'line',
        'source': {
            'type': 'geojson',
            'data': {
                'type': 'FeatureCollection',
                'features': []
                }
        },
        'paint': {
        'line-width': 3,
            'line-color': ['get', 'color']
        }
    };
    arrayPrimitives.forEach( obj => {
        linesOBJ.source.data.features.push(buildLine(obj))
    });

    return linesOBJ
}

function buildLayer(obj) {
    return {
        'id': getRandomID(),
        'type': 'fill',
        'source': {
            'type': 'geojson',
            'data': {
                'type': 'Feature',
                'geometry': {
                    'type': obj.type,
                    'coordinates': obj.coordinates
                }
            }
        },
        'layout': {},
        'paint': {
            'fill-color': '#FF0101',
            'fill-opacity': 0.8
        }
    }
}

