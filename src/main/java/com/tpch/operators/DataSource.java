package com.tpch.operators;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.concurrent.TimeUnit;

public class DataSource implements SourceFunction<String>  {
    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        long startTime = System.currentTimeMillis(); //
        try (BufferedReader reader = new BufferedReader(new FileReader("data/i_withtag0.25_break.txt"))) {
            String line = reader.readLine();
            while (line != null) {
                if (line.equals("Bye") || line.equals("End")) {
                    long endTime = System.currentTimeMillis();
                    System.out.println("===== Period Duration: " + (endTime - startTime) + "ms ======");
                    if (line.equals("Bye")) {
                        TimeUnit.SECONDS.sleep(5);
                        startTime = System.currentTimeMillis();
                    }
                } else {
                    ctx.collect(line);
                }
                line = reader.readLine();
            }
        }
    }

    @Override
    public void cancel() {

    }
}
