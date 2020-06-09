/**
 * Copyright 2019-2020 
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package wfc.examples;

import wfc.resourceAllocators.ExContainerAllocationPolicy;
import wfc.core.WFCDatacenter;
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.text.DecimalFormat;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.DoubleSummaryStatistics;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import static java.util.stream.Collectors.toMap;
import javafx.collections.transformation.SortedList;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletSchedulerSpaceShared;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.EventInfo;
import org.cloudbus.cloudsim.EventListener;
import org.cloudbus.cloudsim.HarddriveStorage;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.VmAllocationPolicy;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.container.resourceAllocators.*;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerBwProvisionerSimple;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerPe;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerPeProvisioner;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerPeProvisionerSimple;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerRamProvisionerSimple;
import org.cloudbus.cloudsim.container.containerVmProvisioners.ContainerVmBwProvisionerSimple;
import org.cloudbus.cloudsim.container.containerVmProvisioners.ContainerVmPe;
import org.cloudbus.cloudsim.container.containerVmProvisioners.ContainerVmPeProvisionerSimple;
import org.cloudbus.cloudsim.container.containerVmProvisioners.ContainerVmRamProvisionerSimple;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import org.workflowsim.CondorVM;
import org.workflowsim.Task;
import org.workflowsim.WorkflowDatacenter;
import org.workflowsim.Job;
import wfc.workflow.WFCEngine;
import wfc.workflow.WFCPlanner;
import org.workflowsim.utils.ClusteringParameters;
import org.workflowsim.utils.OverheadParameters;
import org.workflowsim.utils.Parameters;
import org.workflowsim.utils.WFCReplicaCatalog;
import org.workflowsim.utils.Parameters.ClassType;
import org.cloudbus.cloudsim.container.core.*;
import org.cloudbus.cloudsim.container.hostSelectionPolicies.HostSelectionPolicy;
import org.cloudbus.cloudsim.container.hostSelectionPolicies.HostSelectionPolicyFirstFit;
import org.cloudbus.cloudsim.container.resourceAllocatorMigrationEnabled.PowerContainerVmAllocationPolicyMigrationAbstractHostSelection;
import org.cloudbus.cloudsim.container.schedulers.*;
import org.cloudbus.cloudsim.container.utils.IDs;
import org.cloudbus.cloudsim.container.vmSelectionPolicies.PowerContainerVmSelectionPolicy;
import org.cloudbus.cloudsim.container.vmSelectionPolicies.PowerContainerVmSelectionPolicyMinimumMigrationTime;
import wfc.core.*;
import org.cloudbus.cloudsim.examples.container.*;
import org.cloudbus.cloudsim.util.Conversion;
import org.workflowsim.failure.FailureGenerator;
import org.workflowsim.failure.FailureMonitor;
import org.workflowsim.failure.FailureParameters;
import org.workflowsim.utils.DistributionGenerator;
import org.cloudbus.cloudsim.util.TimeUtil;
import java.util.*;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Map.Entry;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.*;
import org.cloudbus.cloudsim.network.GraphReaderBrite;
import org.cloudbus.cloudsim.network.topologies.*;

/**
 * This WorkflowSimExample creates a workflow planner, a workflow engine, and
 * one schedulers, one data centers and 20 vms. You should change daxPath at
 * least. You may change other parameters as well.
 *
 * @author Arman Riazi
 * @since WFC Pluse Toolkit 1.0
 * @date Apr 1, 2019
 */
/*
WFCConstants.WFC_DC_SCHEDULING_INTERVAL+ 0.1D

Workflow (  
    ClusteringMethod.NONE
    SchedulingAlgorithm.MINMIN
    PlanningAlgorithm.INVALID
    FileSystem.LOCAL
)

On ContainerHost (
    Scheduler = ContainerSchedulerTimeSharedOverSubscription    
    AllocationPolicy = ExContainerAllocationPolicy
    Pe = PeProvisionerSimple 
)

On Container (
    Allocation = ContainerAllocationPolicySimple
    Scheduler = ContainerCloudletSchedulerTimeShared 
    UtilizationModelFull
)
 */

public class WFCExample {

    private static final String NETWORK_TOPOLOGY_FILE = "topology.brite";
    private static String experimentName = "WFCPlusExample_DynamicAndStatic_NoVm";
    private static int num_user = wfc.core.WFCConstants.WFC_NUMBER_USERS;
    private static boolean trace_flag = false;  // mean trace events
    private static boolean failure_flag = false;
    private static List<Container> containerList;    
    
    /**
     * The hostList.
     */
    public static List<ContainerHost> hostList;  
    /**
     * Creates main() to run this example This example has only one datacenter
     * and one storage
     */
    public static void main(String[] args) {

        try {

            WFCConstants.CAN_PRINT_SEQ_LOG = false;
            WFCConstants.CAN_PRINT_SEQ_LOG_Just_Step = false;
            WFCConstants.ENABLE_OUTPUT = false;
            WFCConstants.ENABLE_NETWORK = true;
            WFCConstants.FAILURE_FLAG = failure_flag;
            WFCConstants.RUN_AS_STATIC_RESOURCE = true;
            WFCConstants.POWER_MODE = false;

            FailureParameters.FTCMonitor ftc_monitor = null;
            FailureParameters.FTCFailure ftc_failure = null;
            FailureParameters.FTCluteringAlgorithm ftc_method = null;
            DistributionGenerator[][] failureGenerators = null;

            Log.printLine("Starting " + experimentName + " ... ");

            String daxPath = "./config/dax/Montage_" + (WFCConstants.WFC_NUMBER_CLOUDLETS - 1) + ".xml";

            File daxFile = new File(daxPath);
            if (!daxFile.exists()) {
                Log.printLine("Warning: Please replace daxPath with the physical path in your working environment!");
                return;
            }

            if (failure_flag) {
                /*
                *  Fault Tolerant Parameters
                 */
                /**
                 * MONITOR_JOB classifies failures based on the level of jobs;
                 * MONITOR_VM classifies failures based on the vm id;
                 * MOINTOR_ALL does not do any classification; MONITOR_NONE does
                 * not record any failiure.
                 */
                ftc_monitor = FailureParameters.FTCMonitor.MONITOR_ALL;
                /**
                 * Similar to FTCMonitor, FTCFailure controls the way how we
                 * generate failures.
                 */
                ftc_failure = FailureParameters.FTCFailure.FAILURE_ALL;
                /**
                 * In this example, we have no clustering and thus it is no need
                 * to do Fault Tolerant Clustering. By default, WorkflowSim will
                 * just rety all the failed task.
                 */
                ftc_method = FailureParameters.FTCluteringAlgorithm.FTCLUSTERING_NOOP;
                /**
                 * Task failure rate for each level
                 *
                 */
                failureGenerators = new DistributionGenerator[1][1];
                failureGenerators[0][0] = new DistributionGenerator(DistributionGenerator.DistributionFamily.WEIBULL,
                        100, 1.0, 30, 300, 0.78);
            }

            Parameters.SchedulingAlgorithm sch_method = null;//Parameters.SchedulingAlgorithm.MINMIN;//local
            Parameters.PlanningAlgorithm pln_method = Parameters.PlanningAlgorithm.INVALID;//global-stage
            WFCReplicaCatalog.FileSystem file_system = WFCReplicaCatalog.FileSystem.LOCAL;

            //Parameters.setCostModel(Parameters.CostModel.DATACENTER);
            OverheadParameters op = new OverheadParameters(0, null, null, null, null, 0);

            ClusteringParameters.ClusteringMethod method = ClusteringParameters.ClusteringMethod.NONE;
            ClusteringParameters cp = new ClusteringParameters(0, 0, method, null);

            if (failure_flag) {
                FailureParameters.init(ftc_method, ftc_monitor, ftc_failure, failureGenerators);
            }

            Parameters.init(WFCConstants.WFC_NUMBER_CONTAINER, daxPath, null,
                    null, op, cp, sch_method, pln_method,
                    null, 0); // used WFCPlanner for PlanningAlgorithm
            WFCReplicaCatalog.init(file_system);

            if (failure_flag) {
                FailureMonitor.init();
                FailureGenerator.init();
            }

            Calendar calendar = Calendar.getInstance();

            CloudSim.init(num_user, calendar, trace_flag);

            ExContainerAllocationPolicy containerAllocationPolicy = new ExContainerAllocationPolicy();
            //PowerContainerVmSelectionPolicy vmSelectionPolicy = new PowerContainerVmSelectionPolicyMaximumUsage();
            //HostSelectionPolicy hostSelectionPolicy = new HostSelectionPolicyFirstFit();

            String logAddress = "~/Results";

            //cloudletList = new ArrayList<ContainerCloudlet>();
            containerList = new ArrayList<Container>();

            hostList = new ArrayList<ContainerHost>();
            hostList = createHostList(WFCConstants.WFC_NUMBER_HOSTS, WFCConstants.RUN_AS_STATIC_RESOURCE);
 
            @SuppressWarnings("unused")
            WFCDatacenter datacenter = (WFCDatacenter) createDatacenter("datacenter_0",
                    WFCDatacenter.class, hostList, containerAllocationPolicy,
                    getExperimentName(experimentName, String.valueOf(WFCConstants.OVERBOOKING_FACTOR)),
                    WFCConstants.WFC_DC_SCHEDULING_INTERVAL, logAddress,
                    WFCConstants.WFC_CONTAINER_STARTTUP_DELAY);

            WFCPlanner wfPlanner = new WFCPlanner("planner_0", WFCConstants.WFC_NUMBER_SCHEDULER);

            WFCEngine wfEngine = wfPlanner.getWorkflowEngine();
                                  
            wfEngine.bindSchedulerDatacenter(datacenter.getId(), 0);

            
            CloudSim.terminateSimulation(WFCConstants.SIMULATION_LIMIT);
            
            if(WFCConstants.ENABLE_NETWORK)
               configureNetwork(datacenter.getId(),wfEngine.getSchedulerId(0));
            
            CloudSim.startSimulation();
            
            List<Job> outputList0 = wfEngine.getJobsReceivedList();

            printJobList(outputList0, datacenter);
            

            CloudSim.stopSimulation();
            Log.printLine(experimentName + "finished!");

            //outputByRunnerAbs();
        } catch (Exception e) {
            e.printStackTrace();
            Log.printLine("The simulation has been terminated due to an unexpected error");
            Log.printLine(e.getMessage());
            System.exit(0);
        }

    }



    public static WFCDatacenter createDatacenter(String name, Class<? extends WFCDatacenter> datacenterClass,
            List<ContainerHost> hostList,
            ContainerAllocationPolicy containerAllocationPolicy,
            String experimentName, double schedulingInterval, String logAddress,
            double containerStartupDelay) throws Exception {

        // 4. Create a DatacenterCharacteristics object that stores the
        //    properties of a data center: architecture, OS, list of
        //    Machines, allocation policy: time- or space-shared, time zone
        //    and its price (G$/Pe time unit).
        LinkedList<Storage> storageList = new LinkedList<Storage>();
        WFCDatacenter datacenter = null;

        // 5. Finally, we need to create a storage object.
        /**
         * The bandwidth within a data center in MB/s.
         */
        //int maxTransferRate = 15;// the number comes from the futuregrid site, you can specify your bw
        try {
            // Here we set the bandwidth to be 15MB/s
            HarddriveStorage s1 = new HarddriveStorage(name, 1e12);
            s1.setMaxTransferRate(WFCConstants.WFC_DC_MAX_TRANSFER_RATE);
            storageList.add(s1);

            ContainerDatacenterCharacteristics characteristics = new ContainerDatacenterCharacteristics(WFCConstants.WFC_DC_ARCH, WFCConstants.WFC_DC_OS, WFCConstants.WFC_DC_VMM,
                    hostList, WFCConstants.WFC_DC_TIME_ZONE, WFCConstants.WFC_DC_COST, WFCConstants.WFC_DC_COST_PER_MEM,
                    WFCConstants.WFC_DC_COST_PER_STORAGE, WFCConstants.WFC_DC_COST_PER_BW);
            /*
                        String name,
            ContainerDatacenterCharacteristics characteristics,            
            ContainerAllocationPolicy containerAllocationPolicy,
            List<Storage> storageList,
            double schedulingInterval, String experimentName, String logAddress)
             */

            datacenter = new WFCDatacenter(name, characteristics, hostList,
                    containerAllocationPolicy, storageList, experimentName, schedulingInterval, logAddress, containerStartupDelay);
//                VMStartupDelay, ContainerStartupDelay);

        } catch (Exception e) {
            e.printStackTrace();
            Log.printLine("The simulation has been terminated due to an unexpected error");
            Log.printLine(e.getMessage());
            System.exit(0);
        }
        return datacenter;
    }

    public static List<ContainerHost> createHostList(int hostsNumber, boolean staticRes) {
        ArrayList<ContainerHost> hostList = new ArrayList<ContainerHost>();
        for (int i = 0; i < hostsNumber; ++i) {
            int hostType = i / (int) Math.ceil((double) hostsNumber / 3.0D);

            ArrayList<ContainerPe> peList = new ArrayList<ContainerPe>();
            // 3. Create PEs and add these into the list.
            //for a quad-core machine, a list of 4 PEs is required:

            if (staticRes) {
                for (int p = 0; p < WFCConstants.WFC_NUMBER_HOST_PES; ++p) {

                    peList.add(new ContainerPe(p, new ContainerPeProvisionerSimple(WFCConstants.WFC_HOST_MIPS))); // need to store Pe id and MIPS Rating            
                }
                hostList.add(
                        new ContainerHost(IDs.pollId(ContainerHost.class),
                                new ContainerRamProvisionerSimple(WFCConstants.WFC_HOST_RAM),
                                new ContainerBwProvisionerSimple(WFCConstants.WFC_HOST_BW),
                                WFCConstants.WFC_HOST_STORAGE,
                                peList,
                                new ContainerSchedulerTimeSharedOverSubscription(peList)
                        //new ContainerSchedulerTimeShared(peList)
                        ));
            } else {
                for (int p = 0; p < WFCConstants.HOST_PES[hostType]; ++p) {

                    peList.add(new ContainerPe(p, new ContainerPeProvisionerSimple(WFCConstants.HOST_MIPS[hostType]))); // need to store Pe id and MIPS Rating            
                }

                hostList.add(
                        new ContainerHost(IDs.pollId(ContainerHost.class),
                                new ContainerRamProvisionerSimple(WFCConstants.HOST_RAM[hostType]),
                                new ContainerBwProvisionerSimple(WFCConstants.HOST_BW[hostType]),
                                WFCConstants.HOST_STORAGE,
                                peList,
                                new ContainerSchedulerTimeSharedOverSubscription(peList)
                        //new ContainerSchedulerTimeShared(peList)
                        ));
            }

        }

        return hostList;
    }

    private static String getExperimentName(String... args) {
        StringBuilder experimentName = new StringBuilder();

        for (int i = 0; i < args.length; ++i) {
            if (!args[i].isEmpty()) {
                if (i != 0) {
                    experimentName.append("_");
                }

                experimentName.append(args[i]);
            }
        }

        return experimentName.toString();
    }

    
    
    public static List<Container> createContainerList(int brokerId, int containersNumber, boolean staticRes) {
        LinkedList<Container> list = new LinkedList<>();
        try {
            Container[] containers = new Container[containersNumber];
            for (int i = 0; i < containersNumber; i++) {
                int containerType = i / (int) Math.ceil((double) containersNumber / 3.0D);

                /*for (int p = 0; p <  WFCConstants.CONTAINER_PES[containerType]; ++p) {
                  peList.add(new ContainerPe(p, new ContainerPeProvisioner(WFCConstants.CONTAINER_MIPS[containerType]))); // need to store Pe id and MIPS Rating            
                } */
                if (staticRes) {
                    containers[i] = new Container(IDs.pollId(Container.class), brokerId,
                            (double) WFCConstants.WFC_CONTAINER_MIPS,
                            WFCConstants.WFC_CONTAINER_PES_NUMBER,
                            (int) WFCConstants.WFC_CONTAINER_RAM,
                            (int) WFCConstants.WFC_CONTAINER_BW,
                            (long) WFCConstants.WFC_CONTAINER_SIZE,
                            WFCConstants.WFC_CONTAINER_VMM,
                            new ContainerCloudletSchedulerTimeShared(), WFCConstants.WFC_DC_SCHEDULING_INTERVAL);
                    //  new ContainerCloudletSchedulerDynamicWorkload(WFCConstants.WFC_CONTAINER_MIPS, WFCConstants.WFC_CONTAINER_PES_NUMBER),WFCConstants.WFC_DC_SCHEDULING_INTERVAL);                    
                } else {
                    containers[i] = new Container(IDs.pollId(Container.class), brokerId,
                            (double) WFCConstants.CONTAINER_MIPS[containerType],
                            WFCConstants.CONTAINER_PES[containerType],
                            (int) WFCConstants.CONTAINER_RAM[containerType],
                            (int) WFCConstants.CONTAINER_BW[containerType],
                            (long) WFCConstants.CONTAINER_SIZE,
                            WFCConstants.WFC_CONTAINER_VMM,
                            new ContainerCloudletSchedulerTimeShared(), WFCConstants.WFC_DC_SCHEDULING_INTERVAL);
                    //new ContainerCloudletSchedulerDynamicWorkload(WFCConstants.CONTAINER_MIPS[containerType], WFCConstants.CONTAINER_PES[containerType]),WFCConstants.WFC_DC_SCHEDULING_INTERVAL);                    
                }
                list.add(containers[i]);
            }
        } catch (Exception e) {
            e.printStackTrace();
            Log.printLine("The simulation has been terminated due to an unexpected error");
            Log.printLine(e.getMessage());
            System.exit(0);
        }
        return list;

    }
        
    private static void configureNetwork(int datacenterId,int brokerId) {
         //load the network topology file
        NetworkTopology networkTopology = BriteNetworkTopology.getInstance(NETWORK_TOPOLOGY_FILE);
        
        //networkTopology.buildNetworkTopology(NETWORK_TOPOLOGY_FILE);        
        int briteNode = 0;                
        networkTopology.mapNode(datacenterId, briteNode);

        //Broker will correspond to BRITE node 3
        briteNode = 3;
        networkTopology.mapNode(brokerId, briteNode);
        
        //CloudSim.networkTopology =org.cloudbus.cloudsim.NetworkTopology;

    }
    
    
    /**
    /**
     * Gets the maximum number of GB ever used by the application's heap.
     * @return the max heap utilization in GB
     * @see <a href="https://www.oracle.com/webfolder/technetwork/tutorials/obe/java/gc01/index.html">Java Garbage Collection Basics (for information about heap space)</a>
     */
    private static double getMaxHeapUtilizationGB() {
        final double memoryBytes =
            ManagementFactory.getMemoryPoolMXBeans()
                             .stream()
                             .filter(bean -> bean.getType() == MemoryType.HEAP)
                             .filter(bean -> bean.getName().contains("Eden Space") || bean.getName().contains("Survivor Space"))
                             .map(MemoryPoolMXBean::getPeakUsage)
                             .mapToDouble(MemoryUsage::getUsed)
                             .sum();

        return Conversion.bytesToGigaBytes(memoryBytes);
    }

 
    /*
     static ArrayList<LinkedList<Integer>> StaticGroupingOutHostList;
     static ArrayList<LinkedList<Integer>> StaticGroupingOutContainerList;
     static int StaticPrevCounterHosts;
     static int StaticPrevCounterContainers;
     static int StaticTempPrevHostId;  
     static int StaticTempPrevContainerId;
     */
    protected static void printJobList(List<Job> list, WFCDatacenter datacenter) {
        
        double maxHeapUtilizationGB = getMaxHeapUtilizationGB();
        String indent = "    ";
        double cost = 0.0;
        double time = 0.0;
        double length = 0.0;
        int counter = 1;
        int success_counter = 0;
        int failed_counter = 0;
        /*
        StaticGroupingOutHostList=new ArrayList<LinkedList<Integer>>();
        StaticGroupingOutHostList.add(new LinkedList<Integer>());
        StaticGroupingOutHostList.add(new LinkedList<Integer>());
        StaticGroupingOutContainerList=new ArrayList<LinkedList<Integer>>();
        StaticGroupingOutContainerList.add(new LinkedList<Integer>());
        StaticGroupingOutContainerList.add(new LinkedList<Integer>());
        
        StaticPrevCounterHosts=0;
        StaticPrevCounterContainers=0;        
        StaticTempPrevHostId=WFCConstants.WFC_NUMBER_HOSTS-1;  
        StaticTempPrevContainerId=WFCConstants.WFC_NUMBER_CONTAINER-1;        
         */
        //        
        Log.printLine();
        Log.printLine("========== OUTPUT ==========");
        Log.printLine("Cloudlet Column=Task=>Length,WFType,Impact # Times of Task=>Actual,Exec,Finish.");//,CloudletOutputSize
        Log.printLine();
        Log.printLine(indent + "Row" + indent + "JOB ID" + indent + indent + "CLOUDLET" + indent + indent
                + "STATUS" + indent
                + "Data CENTER ID" + indent
                //+ "HOST ID" + indent 
                + "CONTAINER(ID" + indent + indent + "SIZE)" + indent + indent
                + "TIME" + indent + indent + "START TIME" + indent + indent + "FINISH TIME" + indent + "DEPTH" + indent + indent + "COST");
        //+ indent + indent + "VM (NUM FREE PES" + indent + indent + "NUM OF PES" + indent + indent + "MAX AVAIL MIPS ) ");

        DecimalFormat dft0 = new DecimalFormat("###.#");
        DecimalFormat dft = new DecimalFormat("####.###");

       
            
        for (Job job : list) {
            Log.print(String.format("%6d |-", counter++) + indent + job.getCloudletId() + indent + indent);
            if (job.getClassType() == ClassType.STAGE_IN.value) {
                Log.print("STAGE-IN");
            }
            for (Task task : job.getTaskList()) {

                Log.print(task.getCloudletId() + " ,");
                Log.print(task.getCloudletLength() + " ,");
                Log.print(task.getType());

                //Log.print(dft0.format(task.getImpact()));
                Log.print("\n" + "\t\t\t (" + dft0.format(task.getActualCPUTime()) + " ,");
                Log.print("\n" + "\t\t\t" + dft0.format(task.getExecStartTime()) + " ,");
                Log.print("\n" + "\t\t\t" + dft0.format(task.getTaskFinishTime()) + " )");

            }
            Log.print(indent);

            //ContainerHost host= datacenter.getVmAllocationPolicy().getHost(job.getVmId(), job.getUserId());                              
            
            ContainerHost host = datacenter.getContainerAllocationPolicy().getContainerHost(job.getContainerId(), job.getUserId());
            Container container = host.getContainer(job.getContainerId(), job.getUserId());

            cost += job.getProcessingCost();
            time += job.getActualCPUTime();
            length += job.getCloudletLength();

            if (job.getCloudletStatus() == Cloudlet.SUCCESS) {

                Log.print("     SUCCESS");
                success_counter++;
                //datacenter.getContainerAllocationPolicy().getContainerVm(job.getContainerId(), job.getUserId()).getHost().getId()
                ////datacenter.getVmAllocationPolicy().getHost(job.getVmId(), job.getUserId()).getId()
                Log.printLine(indent + indent + indent + job.getResourceId()
                        //+ indent + indent  + indent + indent + host.getId()                        
                        + indent + indent + indent + job.getContainerId()
                        + indent + indent + indent + container.getSize()
                        + indent + indent + indent + dft.format(job.getActualCPUTime())
                        + indent + indent + indent + dft.format(job.getExecStartTime()) + indent + indent + indent
                        + dft.format(job.getFinishTime()) + indent + indent + indent + job.getDepth()
                        + indent + indent + indent + dft.format(job.getProcessingCost())
                );

                //groupingHost(host,job);
                //groupingContainer(container, job);
            } else if (job.getCloudletStatus() == Cloudlet.FAILED) {
                Log.print("      FAILED");
                failed_counter++;
                Log.printLine(indent + indent + job.getResourceId()
                        //+ indent + indent  + indent + indent + host.getId()
                        + indent + indent + indent + job.getHostId()
                        + indent + indent + indent + job.getContainerId()
                        + indent + indent + indent + container.getSize()
                        + indent + indent + indent + dft.format(job.getActualCPUTime())
                        + indent + indent + indent + dft.format(job.getExecStartTime()) + indent + indent + indent
                        + dft.format(job.getFinishTime()) + indent + indent + indent + job.getDepth()
                        + indent + indent + indent + dft.format(job.getProcessingCost()
                        ));
            }
        }

        //printGroupingHost(StaticGroupingOutHostList);
        //printGroupingContainer(StaticGroupingOutContainerList);
        //ResetPrintGrouping();       
        
        Log.printLine();
        Log.printLine("MinTimeBetweenEvents is " + dft.format(CloudSim.getMinTimeBetweenEvents()));
        Log.printLine("Used MaxHeapUtilization/GB is " + dft.format(maxHeapUtilizationGB));
        Log.printLine("----------------------------------------");
        Log.printLine("The total cost is " + dft.format(cost));
        Log.printLine("The total actual cpu time is " + dft.format(time));
        Log.printLine("The length cloudlets is " + dft.format(length));
        Log.printLine("The total failed counter is " + dft.format(failed_counter));
        Log.printLine("The total success counter is " + dft.format(success_counter));

        
    }

}
    
//Help Comments:
    //vmList = new ArrayList<ContainerVm>();
    /*
    ContainerVmAllocationPolicy vmAllocationPolicy = new
            PowerContainerVmAllocationPolicyMigrationAbstractHostSelection(hostList, vmSelectionPolicy,
            hostSelectionPolicy, WFCConstants.WFC_CONTAINER_OVER_UTILIZATION_THRESHOLD, WFCConstants.WFC_CONTAINER_UNDER_UTILIZATION_THRESHOLD);        
     */
      //vmList = createVmList(wfEngine.getSchedulerId(0), Parameters.getVmNum());                        
    //wfEngine.submitVmList(wfEngine.getVmList(), 0); 
    /*
    
       
    //private UtilizationModelStochastic um;
    ////////////////////////// STATIC METHODS ///////////////////////

    
   private static void groupingHost(ContainerHost host, Job job)
   {        
 
        if(StaticTempPrevHostId != host.getId() || job.containerId== WFCConstants.WFC_NUMBER_CLOUDLETS-1){                            
            StaticGroupingOutHostList.get(0).add(StaticTempPrevHostId);
            StaticGroupingOutHostList.get(1).add(StaticPrevCounterHosts);          
           Log.printLine("------------------------------------------------"
                   + "---------------------------------------------------------------"
                   + "----------------------------------------------------------------------------");
           Log.printConcat("  Host Id :", StaticTempPrevHostId," Sum Group is :",StaticPrevCounterHosts,"\n");       
           Log.printLine("------------------------------------------------"
                   + "---------------------------------------------------------------"
                   + "----------------------------------------------------------------------------");                                                     

           StaticTempPrevHostId = host.getId();                                                 
           StaticPrevCounterHosts =1;
          }
           else 
              StaticPrevCounterHosts++;

     
   }
   
    private static void groupingContainer(Container container, Job job)
   {                 
             
        if(StaticTempPrevContainerId != container.getId() || job.containerId== WFCConstants.WFC_NUMBER_CLOUDLETS-1){                            
                 StaticGroupingOutContainerList.get(0).add(StaticTempPrevContainerId);
                 StaticGroupingOutContainerList.get(1).add(StaticPrevCounterContainers);          
                Log.printLine("------------------------------------------------"
                        + "---------------------------------------------------------------"
                        + "----------------------------------------------------------------------------");
                Log.printConcat("  Container Id :", StaticTempPrevContainerId," Sum Group is :",StaticPrevCounterContainers,"\n");       
                Log.printLine("------------------------------------------------"
                        + "---------------------------------------------------------------"
                        + "----------------------------------------------------------------------------");                                                     

                StaticTempPrevContainerId = container.getId();                                                 
                StaticPrevCounterContainers=1;
               }
                else 
                   StaticPrevCounterContainers++;                        
   }
    
    
    private static void printGroupingHost(ArrayList<LinkedList<Integer>> list){
        Log.printLine();       
        Log.printLine("\nHostId \t\t\tSum Group" );
        
        for(int nk=0;nk<list.get(0).size();nk++){           
                Log.printLine();       
                Log.printConcat(list.get(0).get(nk),"\t\t\t",list.get(1).get(nk));                      
        }     
         Log.printLine(); 
   }
    
   private static void printGroupingContainer(ArrayList<LinkedList<Integer>> list){         
        Log.printLine();       
        Log.printLine("\nContainerId \t\t\tSum Group" );
        
        for(int vk=0;vk<list.get(0).size();vk++){        
                Log.printLine();       
                Log.printConcat(list.get(0).get(vk),"\t\t\t",list.get(1).get(vk));                                     
        }                
        
        Log.printLine();  
   }
   
   private static void ResetPrintGrouping(){         
     StaticGroupingOutHostList=null;
     StaticGroupingOutContainerList=null;
     StaticPrevCounterHosts=0;
     StaticPrevCounterContainers=0;
     StaticTempPrevHostId=0;
     StaticTempPrevContainerId=0;
   }    */
