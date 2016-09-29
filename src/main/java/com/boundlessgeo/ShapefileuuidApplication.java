package com.boundlessgeo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.boundlessgeo.shapefileuuid.service.UUIDInserterService;

@SpringBootApplication
public class ShapefileuuidApplication implements CommandLineRunner {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	
	@Autowired
	UUIDInserterService uis;

	public static void main(String[] args) {
		SpringApplication.run(ShapefileuuidApplication.class, args);
	}

	@Override
	public void run(String... arg0) throws Exception {
		if(arg0.length==1){
			if(arg0[0].contains("shp")){
				String shp = arg0[0].substring(arg0[0].indexOf("=")+1);
				logger.info(shp);
				uis.insertUUIDs("gid",shp);
				logger.info("records processed");
			}else{
				throw new ExitException();
			}
			
		}else{
			throw new ExitException();
		}
		
	}
}
