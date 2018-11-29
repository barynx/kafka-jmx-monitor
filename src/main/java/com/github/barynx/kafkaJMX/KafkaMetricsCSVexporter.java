package com.github.barynx.kafkaJMX;


import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.FileWriter;
import java.io.IOException;

public class KafkaMetricsCSVexporter {


    private CSVPrinter printer;

    public KafkaMetricsCSVexporter() {

    }

    public CSVPrinter getPrinter() {
        return printer;
    }

    public void createCSVFile(String[] headers, String outputFile) throws IOException {
        FileWriter out = new FileWriter(outputFile);
        this.printer = new CSVPrinter(out, CSVFormat.DEFAULT.withHeader(headers));
    }

}
