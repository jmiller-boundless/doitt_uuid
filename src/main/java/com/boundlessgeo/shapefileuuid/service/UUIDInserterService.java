package com.boundlessgeo.shapefileuuid.service;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.geotools.data.DefaultTransaction;
import org.geotools.data.FeatureSource;
import org.geotools.data.Transaction;
import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.geometry.jts.JTS;
import org.opengis.feature.Feature;
import org.opengis.feature.GeometryAttribute;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.vividsolutions.jts.geom.Geometry;

@Service
public class UUIDInserterService {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	public void insertUUIDs(String insertcolumn, String shp) {
		final Transaction transaction = new DefaultTransaction("create");
		final HashMap<String, Serializable> params = new HashMap<>(3);
		final ShapefileDataStoreFactory factory = new ShapefileDataStoreFactory();
		FeatureCollection<SimpleFeatureType, SimpleFeature> out = null;
		try {
			URL shpurl = new File(shp).toURI().toURL();
			params.put(ShapefileDataStoreFactory.URLP.key, shpurl);
			params.put(ShapefileDataStoreFactory.CREATE_SPATIAL_INDEX.key, Boolean.FALSE);
			params.put(ShapefileDataStoreFactory.ENABLE_SPATIAL_INDEX.key, Boolean.FALSE);
			ShapefileDataStore dataStore = (ShapefileDataStore) factory.createDataStore(params);
			String typeName = dataStore.getTypeNames()[0];
			FeatureSource<SimpleFeatureType, SimpleFeature> source= dataStore
			        .getFeatureSource(typeName);
			SimpleFeatureType existingFeatureType = source.getSchema();
			SimpleFeatureBuilder build = new SimpleFeatureBuilder(existingFeatureType);
            SimpleFeatureStore store = (SimpleFeatureStore) dataStore.getFeatureSource( typeName );
            List<SimpleFeature> list = new ArrayList<SimpleFeature>();
			out = source.getFeatures();
			FeatureIterator<SimpleFeature>existingfeatures = out.features();
			while (existingfeatures.hasNext()) {
				SimpleFeature feature = existingfeatures.next();
				for (Property property : feature.getProperties()) {
					if(property.getName().toString().equalsIgnoreCase(insertcolumn)){
						build.set(property.getName(), UUID.randomUUID().toString());
					}else{
						build.set(property.getName(), property.getValue());
					}
				}
                Feature modifiedFeature = build.buildFeature(feature.getIdentifier().getID());
                list.add((SimpleFeature) modifiedFeature);
				
			}
            SimpleFeatureCollection collection = new ListFeatureCollection(existingFeatureType, list);
            store.setTransaction(transaction);
            try {
            	//store.modifyFeatures(existingFeatureType.getDescriptor(insertcolumn).getName(), UUID.randomUUID(), Filter.INCLUDE);
            	store.removeFeatures(Filter.INCLUDE);
            	store.addFeatures(collection);
                transaction.commit();
            } catch (Exception problem) {
                problem.printStackTrace();
                transaction.rollback();
            } finally {
                transaction.close();
            }
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error(e.getLocalizedMessage());

		}catch (Exception e){
			e.printStackTrace();
			logger.error(e.getLocalizedMessage());
		}

	}

}
