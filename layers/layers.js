var wms_layers = [];
var baseLayer = new ol.layer.Group({
    'title': '',
    layers: [
new ol.layer.Tile({
    'title': 'OSM',
    'type': 'base',
    source: new ol.source.OSM()
})
]
});
var format_Ferry_0 = new ol.format.GeoJSON();
var features_Ferry_0 = format_Ferry_0.readFeatures(json_Ferry_0, 
            {dataProjection: 'EPSG:4326', featureProjection: 'EPSG:3857'});
var jsonSource_Ferry_0 = new ol.source.Vector({
    attributions: [new ol.Attribution({html: '<a href=""></a>'})],
});
jsonSource_Ferry_0.addFeatures(features_Ferry_0);var lyr_Ferry_0 = new ol.layer.Vector({
                declutter: true,
                source:jsonSource_Ferry_0, 
                style: style_Ferry_0,
                title: '<img src="styles/legend/Ferry_0.png" /> Ferry'
            });var format_CaminosinPostacin_1 = new ol.format.GeoJSON();
var features_CaminosinPostacin_1 = format_CaminosinPostacin_1.readFeatures(json_CaminosinPostacin_1, 
            {dataProjection: 'EPSG:4326', featureProjection: 'EPSG:3857'});
var jsonSource_CaminosinPostacin_1 = new ol.source.Vector({
    attributions: [new ol.Attribution({html: '<a href=""></a>'})],
});
jsonSource_CaminosinPostacin_1.addFeatures(features_CaminosinPostacin_1);var lyr_CaminosinPostacin_1 = new ol.layer.Vector({
                declutter: true,
                source:jsonSource_CaminosinPostacin_1, 
                style: style_CaminosinPostacin_1,
                title: '<img src="styles/legend/CaminosinPostacin_1.png" /> Camino sin Postación'
            });var format_PostacinElctrica_2 = new ol.format.GeoJSON();
var features_PostacinElctrica_2 = format_PostacinElctrica_2.readFeatures(json_PostacinElctrica_2, 
            {dataProjection: 'EPSG:4326', featureProjection: 'EPSG:3857'});
var jsonSource_PostacinElctrica_2 = new ol.source.Vector({
    attributions: [new ol.Attribution({html: '<a href=""></a>'})],
});
jsonSource_PostacinElctrica_2.addFeatures(features_PostacinElctrica_2);var lyr_PostacinElctrica_2 = new ol.layer.Vector({
                declutter: true,
                source:jsonSource_PostacinElctrica_2, 
                style: style_PostacinElctrica_2,
                title: '<img src="styles/legend/PostacinElctrica_2.png" /> Postación Eléctrica'
            });var format_Fibrapticadetectada_3 = new ol.format.GeoJSON();
var features_Fibrapticadetectada_3 = format_Fibrapticadetectada_3.readFeatures(json_Fibrapticadetectada_3, 
            {dataProjection: 'EPSG:4326', featureProjection: 'EPSG:3857'});
var jsonSource_Fibrapticadetectada_3 = new ol.source.Vector({
    attributions: [new ol.Attribution({html: '<a href=""></a>'})],
});
jsonSource_Fibrapticadetectada_3.addFeatures(features_Fibrapticadetectada_3);var lyr_Fibrapticadetectada_3 = new ol.layer.Vector({
                declutter: true,
                source:jsonSource_Fibrapticadetectada_3, 
                style: style_Fibrapticadetectada_3,
                title: '<img src="styles/legend/Fibrapticadetectada_3.png" /> Fibra Óptica detectada'
            });

lyr_Ferry_0.setVisible(true);lyr_CaminosinPostacin_1.setVisible(true);lyr_PostacinElctrica_2.setVisible(true);lyr_Fibrapticadetectada_3.setVisible(true);
var layersList = [baseLayer,lyr_Ferry_0,lyr_CaminosinPostacin_1,lyr_PostacinElctrica_2,lyr_Fibrapticadetectada_3];
lyr_Ferry_0.set('fieldAliases', {'id': 'id', });
lyr_CaminosinPostacin_1.set('fieldAliases', {'id': 'id', });
lyr_PostacinElctrica_2.set('fieldAliases', {'id': 'id', 'id_2': 'id_2', });
lyr_Fibrapticadetectada_3.set('fieldAliases', {'id': 'id', 'id_2': 'id_2', 'id_3': 'id_3', 'id_2_2': 'id_2_2', 'id_4': 'id_4', });
lyr_Ferry_0.set('fieldImages', {'id': 'TextEdit', });
lyr_CaminosinPostacin_1.set('fieldImages', {'id': 'TextEdit', });
lyr_PostacinElctrica_2.set('fieldImages', {'id': 'TextEdit', 'id_2': 'TextEdit', });
lyr_Fibrapticadetectada_3.set('fieldImages', {'id': 'TextEdit', 'id_2': 'TextEdit', 'id_3': 'TextEdit', 'id_2_2': 'TextEdit', 'id_4': 'TextEdit', });
lyr_Ferry_0.set('fieldLabels', {'id': 'no label', });
lyr_CaminosinPostacin_1.set('fieldLabels', {'id': 'no label', });
lyr_PostacinElctrica_2.set('fieldLabels', {'id': 'no label', 'id_2': 'no label', });
lyr_Fibrapticadetectada_3.set('fieldLabels', {'id': 'no label', 'id_2': 'no label', 'id_3': 'no label', 'id_2_2': 'no label', 'id_4': 'no label', });
lyr_Fibrapticadetectada_3.on('precompose', function(evt) {
    evt.context.globalCompositeOperation = 'normal';
});