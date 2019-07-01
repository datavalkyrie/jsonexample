/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datawhisperers.jsonexample;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroFactory;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import com.fasterxml.jackson.dataformat.avro.schema.AvroSchemaGenerator;
import com.google.gson.Gson;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

/**
 *
 * @author steveo
 */
public class NewMain {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws JsonMappingException {
        // TODO code application logic here
        NewMain newMain = new NewMain();
        newMain.doWork();
    }

    public void doWork() throws JsonMappingException {

        Car car = new Car();
        car.setMake("ford");
        car.setModel("mustang");
        car.setCost(new BigDecimal("70000"));

        Gson gson = new Gson();
        String json = gson.toJson(car);
        System.out.println("JSON:" + json);
        String json2 = "{\"make\":\"ford\",\"model\":\"mustang\",\"cost\":70000}";

        Car car2 = gson.fromJson(json2.toString(), Car.class);
        car2.setRaw(json2);

        //Car2 car3 = gson.fromJson(json2.toString(), Car2.class);
        Car2 car22 = new Car2();

        car22.setCost(car.getCost());
        car22.setRaw(car2.getRaw());

        car.setMake("Ford");

        System.out.println(car);
        System.out.println(car2);
        System.out.println(car22);

        ObjectMapper mapper = new ObjectMapper(new AvroFactory());
        AvroSchemaGenerator gen = new AvroSchemaGenerator();
        mapper.acceptJsonFormatVisitor(Car.class, gen);
        AvroSchema schemaWrapper = gen.getGeneratedSchema();
        org.apache.avro.Schema avroSchema = schemaWrapper.getAvroSchema();
        String asJson = avroSchema.toString(true);

        //System.out.println(asJson);
        File avroOutput = new File("/Users/steveo/Car2-test.avro");
        try {
            DatumWriter<Car2> car2DatumWriter = new SpecificDatumWriter<Car2>(Car2.class);
            DataFileWriter<Car2> dataFileWriter = new DataFileWriter<Car2>(car2DatumWriter);
            dataFileWriter.create(car22.getSchema(), avroOutput);
            dataFileWriter.append(car22);
            dataFileWriter.append(car22);
            dataFileWriter.append(car22);
            //dataFileWriter.append(p2);
            dataFileWriter.close();

        } catch (IOException e) {
            System.out.println("Error writing Avro");
        }

        try {
            DatumReader<Car2> car2DatumReader = new SpecificDatumReader(Car2.class);
            DataFileReader<Car2> dataFileReader = new DataFileReader<Car2>(avroOutput, car2DatumReader);
            Car2 p = null;
            while (dataFileReader.hasNext()) {
                p = dataFileReader.next(p);
                System.out.println(p);
            }
        } catch (IOException e) {
            System.out.println("Error reading Avro");
        }

    }
}
