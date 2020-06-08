
function showInterpolation() {
    map.addSource('interpolation', {
        type: 'geojson',
        data: './json/interpolation.geojson',
        cluster: true,
        clusterMaxZoom: 14, // Max zoom to cluster points on
        clusterRadius: 50 // Radius of each cluster when clustering points (defaults to 50)
    });

    map.addLayer({
        id: 'clusters',
        type: 'circle',
        source: 'interpolation',
        filter: ['!=', ['get', 'id'], 'Novara'],
        paint: {
            'circle-color': 'rgba(213,0,3,0.75)',
            'circle-radius': 20,
        }
    });

    map.addLayer({
        id: 'inferred',
        type: 'circle',
        source: 'interpolation',
        filter: ['==', ['get', 'id'], 'Novara'],
        paint: {
            'circle-color': 'rgba(0,29,204,0.65)',
            'circle-radius': 20,
        }
    });

    map.addLayer({
        id: 'cluster-count',
        type: 'symbol',
        source: 'interpolation',
        layout: {
            'text-field': ['get', 'value'],
            'text-font': ['DIN Offc Pro Medium', 'Arial Unicode MS Bold'],
            'text-size': 15,
        }
    });
}