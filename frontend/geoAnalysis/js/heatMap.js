function showHeatMap() {
    map.addSource('heat', {
        'type': 'geojson',
        'data': './json/dataOutput.geojson'
    });

    map.addLayer(
        {
            'id': 'heat',
            'type': 'heatmap',
            'source': 'heat',
            'maxzoom': 20,
            'paint': {
                'heatmap-weight': 1,
                'heatmap-intensity': 1,
                'heatmap-color': [
                    'interpolate',
                    ['linear'],
                    ['heatmap-density'],
                    0,
                    'rgba(33,102,172,0)',
                    0.2,
                    'rgb(207,202,22)',
                    0.4,
                    'rgb(209,229,240)',
                    0.6,
                    'rgb(253,219,199)',
                    0.8,
                    'rgb(239,138,98)',
                    1,
                    'rgb(178,24,43)'
                ],
                'heatmap-radius': 10,
                'heatmap-opacity': 1
            }
        },
        'waterway-label'
    );

    map.addLayer(
        {
            'id': 'point',
            'type': 'circle',
            'source': 'heat',
            'minzoom': 7,
            'paint': {
                'circle-radius': 4,
                'circle-color': 'red',
                'circle-stroke-color': 'white',
                'circle-stroke-width': 2,
                'circle-opacity': 1
            }
        },
        'waterway-label'
    );
}