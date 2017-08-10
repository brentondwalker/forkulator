package forkulator;

import static org.junit.Assert.*;
import org.junit.Test;
import forkulator.randomprocess.ConstantIntertimeProcess;
import forkulator.randomprocess.ExponentialIntertimeProcess;

public class FJTaskTest {
	
    @Test
    public void testFJTask_Constructor() {
        
        // The constructor requires an IntertimeProcess to set its service time.
        // The ConstantIntertimeProcess is nice for testing because its value is deterministic.
        {
            double rate = 12.345;
            FJTask task = new FJTask(new ConstantIntertimeProcess(rate), 0.0, null);
            assertEquals(1.0/rate, task.service_time, 1e-10);
            assertNull(task.job);
        }
        
        // if you pass a null IntertimeProcess there should be an error
        {
            boolean got_exception = false;
            try{
                @SuppressWarnings("unused")
                FJTask task = new FJTask(null, 0.0, null);
            } catch (NullPointerException e) {
                got_exception = true;
            }
            assertTrue(got_exception);
        }
    }

    @Test
    public void testFJTask_resampleServiceTime() {
        
        // resample the ConstantIntertimeProcess
        {
            double rate = 12.345;
            int numsamples = 1000;
            FJTask task = new FJTask(new ConstantIntertimeProcess(rate), 0.0, null);
            for (int i=0; i<numsamples; i++) {
                task.resampleServiceTime();
            }
            assertEquals(1.0/rate, task.service_time, 1e-10);
        }
        
        // resample an exponential distribution and make sure we get the right mean
        {
            double rate = 0.1;
            int numsamples = 10000;
            double[] samples = new double[numsamples];
            FJTask task = new FJTask(new ExponentialIntertimeProcess(rate), 0.0, null);
            for (int i=0; i<numsamples; i++) {
                task.resampleServiceTime();
                samples[i] = task.service_time;
            }
            assertEquals(1.0/rate, mean(samples), 0.1);
            assertEquals(1.0/rate, Math.sqrt(variance(samples)), 0.5);
        }
    }
    
    static double mean(double[] data) {
        double sum = 0.0;
        for(double a : data)
            sum += a;
        return sum/data.length;
    }

    static double variance(double[] data) {
        double mean = mean(data);
        double temp = 0;
        for(double a : data)
            temp += (a-mean)*(a-mean);
        return temp/(data.length-1);
    }
}
