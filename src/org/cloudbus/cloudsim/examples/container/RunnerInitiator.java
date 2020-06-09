package org.cloudbus.cloudsim.examples.container;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.container.core.*;
import wfc.scheduler.WFCScheduler;
import org.cloudbus.cloudsim.Cloudlet;
import java.util.*;
/**
 * This is the modified version of {@link org.cloudbus.cloudsim.examples.power.planetlab.PlanetLabRunner} in CloudSim Package.
 * Created by sareh on 18/08/15.
 */

public class RunnerInitiator extends RunnerAbs {


    /**
     * Instantiates a new runner.
     *
     * @param enableOutput       the enable output
     * @param outputToFile       the output to file
     * @param inputFolder        the input folder
     * @param outputFolder       the output folder
     *                           //     * @param workload the workload
     * @param vmAllocationPolicy the vm allocation policy
     * @param vmSelectionPolicy  the vm selection policy
     */
    public RunnerInitiator(
            boolean enableOutput,
            boolean outputToFile,
            String inputFolder,
            String outputFolder,
            String vmAllocationPolicy,
            String containerAllocationPolicy,
            String vmSelectionPolicy,
            String containerSelectionPolicy,
            String hostSelectionPolicy,
            double overBookingFactor, String runTime, String logAddress) {


        super(enableOutput,
                outputToFile,
                inputFolder,
                outputFolder,
                vmAllocationPolicy,
                containerAllocationPolicy,
                vmSelectionPolicy,
                containerSelectionPolicy,
                hostSelectionPolicy,
                overBookingFactor, runTime, logAddress);

    }

    /*
     * (non-Javadoc)
     *
     * @see RunnerAbs
     */
    @Override
    protected void init(String inputFolder, double overBookingFactor,
            WFCScheduler<? extends ContainerDatacenterBroker> broker,
            List<? extends Cloudlet> cloudletList,
            List<? extends Container> containerList,
            List<? extends ContainerVm> vmList,
            List<? extends ContainerHost> hostList) {
        try {
            CloudSim.init(1, Calendar.getInstance(), false);
//            setOverBookingFactor(overBookingFactor);
            broker = HelperEx.createBroker(overBookingFactor);
            int brokerId = broker.getId();
            cloudletList = cloudletList;
            containerList = containerList;
            vmList = vmList;
            hostList = hostList;

        } catch (Exception e) {
            e.printStackTrace();
            Log.printLine("The simulation has been terminated due to an unexpected error");
            System.exit(0);
        }
    }


}
