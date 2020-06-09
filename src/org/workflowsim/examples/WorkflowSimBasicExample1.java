/**
 * Copyright 2012-2013 University Of Southern California
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
package org.workflowsim.examples;

import wfc.core.WFCConstants;
import java.io.File;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletSchedulerDynamicWorkload;
import org.cloudbus.cloudsim.CloudletSchedulerSpaceShared;
import org.cloudbus.cloudsim.CloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.HarddriveStorage;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.VmAllocationPolicy;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.VmSchedulerTimeSharedOverSubscription;
import org.cloudbus.cloudsim.container.containerVmProvisioners.ContainerVmBwProvisionerSimple;
import org.cloudbus.cloudsim.container.containerVmProvisioners.ContainerVmPe;
import org.cloudbus.cloudsim.container.containerVmProvisioners.ContainerVmPeProvisionerSimple;
import org.cloudbus.cloudsim.container.containerVmProvisioners.ContainerVmRamProvisionerSimple;
import org.cloudbus.cloudsim.container.core.ContainerHost;
import org.cloudbus.cloudsim.container.core.PowerContainerHostUtilizationHistory;
import org.cloudbus.cloudsim.container.schedulers.ContainerVmSchedulerTimeSharedOverSubscription;
import org.cloudbus.cloudsim.container.utils.IDs;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import org.workflowsim.CondorVM;
import org.workflowsim.Task;
import org.workflowsim.WorkflowDatacenter;
import org.workflowsim.Job;
import org.workflowsim.WorkflowEngine;
import org.workflowsim.DynaResWorkflowPlanner;
import org.workflowsim.utils.ClusteringParameters;
import org.workflowsim.utils.OverheadParameters;
import org.workflowsim.utils.Parameters;
import org.workflowsim.utils.ReplicaCatalog;
import org.workflowsim.utils.Parameters.ClassType;
import org.cloudbus.cloudsim.examples.container.*;
/**
 * This WorkflowSimExample creates a workflow planner, a workflow engine, and
 * one schedulers, one data centers and 20 vms. You should change daxPath at
 * least. You may change other parameters as well.
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 9, 2013
 */
public class WorkflowSimBasicExample1 {
    
    private static int numberMontage;
    private static String experimentName;
    
    protected static List<CondorVM> createVM(int userId, int vms) {
        //Creates a container to store VMs. This list is passed to the broker later
        LinkedList<CondorVM> list = new LinkedList<>();
/*
        //VM Parameters
        long size = 2500; //image size (MB)
        int ram =2048;//1024; //vm memory (MB)
        int mips = 18637;//2000;
        long bw = 100000;
        int pesNumber = 2; //number of cpus
        String vmm = "Xen"; //VMM name

        //create VMs
        CondorVM[] vm = new CondorVM[vms];
        for (int i = 0; i < vms; i++) {
            double ratio = 1.0;
            vm[i] = new CondorVM(i, userId, mips * ratio, pesNumber, ram, bw, size, vmm, new CloudletSchedulerTimeShared());
            list.add(vm[i]);
        }
        */

        for (int i = 0; i < vms; ++i) {
             int vmType = i / (int) Math.ceil((double) vms / 3.0D);

             list.add(new CondorVM(i, userId,
                    (float)WFCConstants.VM_MIPS[vmType],
                    (int) WFCConstants.VM_PES[vmType],
                    (int) WFCConstants.VM_RAM[vmType],
                    WFCConstants.VM_BW,
                    WFCConstants.VM_SIZE, "Xen",
                    new CloudletSchedulerTimeShared()
             ));
        }
        return list;
    }

    
    
     public static List<Host> createHostList(int hostsNumber) {
        ArrayList<Host> hostList = new ArrayList<Host>();
        for (int i = 0; i < hostsNumber; ++i) {
            int hostType = i / (int) Math.ceil((double) hostsNumber / 3.0D);
            ArrayList<Pe> peList = new ArrayList<Pe>();
            for (int j = 0; j < WFCConstants.HOST_PES[hostType]; ++j) {
                peList.add(new Pe(j,
                        new PeProvisionerSimple((double) WFCConstants.HOST_MIPS[hostType])));
            }

             hostList.add(
                    new Host(
                            IDs.pollId(Host.class),
                            new RamProvisionerSimple(WFCConstants.HOST_RAM[hostType]),
                            new BwProvisionerSimple(WFCConstants.HOST_STORAGE),
                            WFCConstants.HOST_STORAGE,
                            peList,
                            new VmSchedulerTimeSharedOverSubscription(peList))
            ); // This is our first machine
        }

        return hostList;
    }
    
    ////////////////////////// STATIC METHODS ///////////////////////
    /**
     * Creates main() to run this example This example has only one datacenter
     * and one storage
     */
    public static void main(String[] args) {
        try {
            // First step: Initialize the WorkflowSim package. 
            /**
             * However, the exact number of vms may not necessarily be vmNum If
             * the data center or the host doesn't have sufficient resources the
             * exact vmNum would be smaller than that. Take care.
             */
            int vmNum = 14;//number of vms;
            /**
             * Should change this based on real physical path
             */
            
            experimentName="WorkflowSimBasicExample1";
            numberMontage=1000;
            
           // handelNumberConstants(numberMontage);
          
            WFCConstants.CAN_PRINT_SEQ_LOG = false;
            WFCConstants.CAN_PRINT_SEQ_LOG_Just_Step = false;
            
            Log.printLine("Starting " + experimentName + " ... ");
                        
            String daxPath = "./config/dax/Montage_" + numberMontage + ".xml";
                        
            File daxFile = new File(daxPath);
            if (!daxFile.exists()) {
                Log.printLine("Warning: Please replace daxPath with the physical path in your working environment!");
                return;
            }

            /**
             * Since we are using MINMIN scheduling algorithm, the planning
             * algorithm should be INVALID such that the planner would not
             * override the result of the scheduler
             */
                       
            Parameters.SchedulingAlgorithm sch_method = Parameters.SchedulingAlgorithm.MINMIN;//local
            Parameters.PlanningAlgorithm pln_method = Parameters.PlanningAlgorithm.INVALID;//global-stage
            ReplicaCatalog.FileSystem file_system = ReplicaCatalog.FileSystem.LOCAL;

            OverheadParameters op = new OverheadParameters(0, null, null, null, null, 0);
   
            ClusteringParameters.ClusteringMethod method = ClusteringParameters.ClusteringMethod.NONE;
            ClusteringParameters cp = new ClusteringParameters(0, 0, method, null);

            Parameters.init(WFCConstants.WFC_NUMBER_VMS, daxPath, null,
                    null, op, cp, sch_method, pln_method,
                    null, 0);
            ReplicaCatalog.init(file_system);


            // before creating any entities.
            int num_user = 1;   // number of grid users
            Calendar calendar = Calendar.getInstance();
            boolean trace_flag = false;  // mean trace eventis

            // Initialize the CloudSim library
            CloudSim.init(num_user, calendar, trace_flag);

            WorkflowDatacenter datacenter = createDatacenter("Datacenter_0");

            /**
             * Create a WorkflowPlanner with one schedulers.
             */
            DynaResWorkflowPlanner wfPlanner = new DynaResWorkflowPlanner("planner_0", 1);
            /**
             * Create a WorkflowEngine.
             */
            WorkflowEngine wfEngine = wfPlanner.getWorkflowEngine();
            /**
             * Create a list of VMs.The userId of a vm is basically the id of
             * the scheduler that controls this vm.
             */
            List<CondorVM> vmlist0 = createVM(wfEngine.getSchedulerId(0), Parameters.getVmNum());

            /**
             * Submits this list of vms to this WorkflowEngine.
             */
            wfEngine.submitVmList(vmlist0, 0);

            /**
             * Binds the data centers with the scheduler.
             */
            wfEngine.bindSchedulerDatacenter(datacenter.getId(), 0);
            
            CloudSim.terminateSimulation(87400.00);
            CloudSim.startSimulation();         
           
            List<Job> outputList0 = wfEngine.getJobsReceivedList();
            printJobList(outputList0); 
            CloudSim.stopSimulation();
        } catch (Exception e) {
            Log.printLine("The simulation has been terminated due to an unexpected error");
            Log.printLine(e.getMessage());
        }
    }

    protected static WorkflowDatacenter createDatacenter(String name) {

        // Here are the steps needed to create a PowerDatacenter:
        // 1. We need to create a list to store one or more
        //    Machines
        
        
        List<Host> hostList = new ArrayList<>();
/*
        // 2. A Machine contains one or more PEs or CPUs/Cores. Therefore, should
        //    create a list to store these PEs before creating
        //    a Machine.
        for (int i = 1; i <= 14; i++) {
            List<Pe> peList1 = new ArrayList<>();
            int mips = 37274 / 2;
            // 3. Create PEs and add these into the list.
            //for a quad-core machine, a list of 4 PEs is required:
            peList1.add(new Pe(0, new PeProvisionerSimple(mips))); // need to store Pe id and MIPS Rating
            peList1.add(new Pe(1, new PeProvisionerSimple(mips)));
        */
/*
            int hostId = 0;
            int ram = 1024; //host memory (MB)
            long storage = 1000000; //host storage
            int bw = 10000;
            hostList.add(
                    new Host(
                            hostId,
                            new RamProvisionerSimple(ram),
                            new BwProvisionerSimple(bw),
                            storage,
                            peList1,
                            new VmSchedulerTimeShared(peList1))
            ); // This is our first machine
             hostList.add(
                    new Host(
                            hostId + 1 ,
                            new RamProvisionerSimple(ram),
                            new BwProvisionerSimple(bw),
                            storage,
                            peList1,
                            new VmSchedulerTimeShared(peList1))
            );
            //hostId++;
            */
        //}

        // 4. Create a DatacenterCharacteristics object that stores the
        //    properties of a data center: architecture, OS, list of
        //    Machines, allocation policy: time- or space-shared, time zone
        //    and its price (G$/Pe time unit).
        String arch = "x86";      // system architecture
        String os = "Linux";          // operating system
        String vmm = "Xen";
        double time_zone = 10.0;         // time zone this resource located
        double cost = 3.0;              // the cost of using processing in this resource
        double costPerMem = 0.05;		// the cost of using memory in this resource
        double costPerStorage = 0.1;	// the cost of using storage in this resource
        double costPerBw = 0.1;			// the cost of using bw in this resource
        LinkedList<Storage> storageList = new LinkedList<>();	//we are not adding SAN devices by now
        WorkflowDatacenter datacenter = null;
    
        hostList = createHostList(WFCConstants.WFC_NUMBER_HOSTS);
        
        DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
                arch, os, vmm, hostList, time_zone, cost, costPerMem, costPerStorage, costPerBw);

        // 5. Finally, we need to create a storage object.
        /**
         * The bandwidth within a data center in MB/s.
         */
        int maxTransferRate = 15;// the number comes from the futuregrid site, you can specify your bw
    
       
        try {
            // Here we set the bandwidth to be 15MB/s
            HarddriveStorage s1 = new HarddriveStorage(name, 1e12);
            s1.setMaxTransferRate(maxTransferRate);
            storageList.add(s1);
            datacenter = new WorkflowDatacenter(name, characteristics, new VmAllocationPolicySimple(hostList), storageList, WFCConstants.WFC_DC_SCHEDULING_INTERVAL);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return datacenter;
    }

    /**
     * Prints the job objects
     *
     * @param list list of jobs
     */
    protected static void printJobList(List<Job> list) {
        String indent = "    ";
        double cost = 0.0;
        double time = 0.0;
        double length=0.0;
        
        Log.printLine();
        Log.printLine("========== OUTPUT ==========");
        Log.printLine(indent + "JOB ID" + indent + "CLOUDLET" + indent + indent +  "STATUS" + indent
                + "Data CENTER ID" + indent + "VM ID"  + indent + indent
                + "TIME" + indent +  indent +"START TIME" + indent + indent + "FINISH TIME" + indent + "DEPTH" + indent + indent + "Cost");
        
        DecimalFormat dft = new DecimalFormat("###.##");
        
        for (Job job : list) {
            Log.print(indent + job.getCloudletId() + indent + indent);
            if (job.getClassType() == ClassType.STAGE_IN.value) {
                Log.print("Stage-in");
            }
            for (Task task : job.getTaskList()) {
                Log.print(task.getCloudletId() + ",");
                Log.print(task.getCloudletLength());               
            }
            Log.print(indent);

            cost += job.getProcessingCost();
            time += job.getActualCPUTime();
            length +=job.getCloudletLength();
            
            if (job.getCloudletStatus() == Cloudlet.SUCCESS) {
                Log.print("     SUCCESS");
                Log.printLine(indent + indent + job.getResourceId() + indent + indent + indent + job.getVmId()
                        + indent + indent + indent + dft.format(job.getActualCPUTime())
                        + indent + indent + dft.format(job.getExecStartTime()) + indent + indent + indent
                        + dft.format(job.getFinishTime()) + indent + indent + indent + job.getDepth()
                        + indent + indent + indent + dft.format(job.getProcessingCost()));
            } else if (job.getCloudletStatus() == Cloudlet.FAILED) {
                Log.print("     FAILED");
                Log.printLine(indent + indent + job.getResourceId() + indent + indent + indent + job.getVmId()
                        + indent + indent + indent + dft.format(job.getActualCPUTime())
                        + indent + indent + dft.format(job.getExecStartTime()) + indent + indent + indent
                        + dft.format(job.getFinishTime()) + indent + indent + indent + job.getDepth()
                        + indent + indent + indent + dft.format(job.getProcessingCost()));
            }
        }
        
        Log.printLine("The total cost is " + dft.format(cost));
        Log.printLine("The total actual cpu time is " + dft.format(time));
        Log.printLine("The length cloudlets is " + dft.format(length));      
    }
    /*
    public static void handelNumberConstants(int numMontage){            
            int numCoefficient=1;
            
            switch (numMontage){
                    case 25:
                        WFCConstants.NUMBER_MONTAGE=25;
                        numCoefficient=1;
                        break;
                    case 50:
                        WFCConstants.NUMBER_MONTAGE=50;
                        numCoefficient=2;
                        break;
                    case 100:
                        WFCConstants.NUMBER_MONTAGE=100;
                        numCoefficient=3;
                        break;
                    default : 
                        WFCConstants.NUMBER_MONTAGE=25;
            }
            WFCConstants.NUMBER_HOSTS = numCoefficient * 2;
            WFCConstants.NUMBER_VMS = numCoefficient * 7;            
            WFCConstants.NUMBER_CLOUDLETS=WFCConstants.NUMBER_MONTAGE;  
      }*/
}
