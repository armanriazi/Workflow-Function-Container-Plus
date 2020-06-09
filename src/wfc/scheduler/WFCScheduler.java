package wfc.scheduler;

import org.cloudbus.cloudsim.container.lists.ContainerList;
import org.cloudbus.cloudsim.container.lists.ContainerList;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.UtilizationModelPlanetLabInMemory;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.lists.CloudletList;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.core.ContainerCloudlet;
import org.cloudbus.cloudsim.container.core.ContainerDatacenterCharacteristics;
import org.cloudbus.cloudsim.container.core.ContainerHost;
import org.cloudbus.cloudsim.container.core.containerCloudSimTags;
import wfc.core.WFCConstants;
import org.workflowsim.Job;
import org.workflowsim.WorkflowSimTags;
import org.workflowsim.failure.FailureGenerator;
import org.workflowsim.scheduling.BaseSchedulingAlgorithm;
import org.workflowsim.scheduling.DataAwareSchedulingAlgorithm;
import org.workflowsim.scheduling.FCFSSchedulingAlgorithm;
import org.workflowsim.scheduling.MCTSchedulingAlgorithm;
import org.workflowsim.scheduling.MaxMinSchedulingAlgorithm;
import org.workflowsim.scheduling.MinMinSchedulingAlgorithm;
import org.workflowsim.scheduling.RoundRobinSchedulingAlgorithm;
import org.workflowsim.scheduling.StaticSchedulingAlgorithm;
import org.workflowsim.utils.Parameters;

/**
 * Created by sareh on 15/07/15.
 * Manipulated by Arman
 */

public class WFCScheduler extends SimEntity {

    private int workflowEngineId;
    
    /**
     * The vm list.
     */
    protected List<? extends ContainerHost> containerHostList;

    /**
     * The vms created list.
     */
    protected List<? extends ContainerHost> containerHostsCreatedList;
/**
     * The containers created list.
     */
    protected List<? extends Container> containersCreatedList;

    /**
     * The cloudlet list.
     */
    protected List<? extends ContainerCloudlet> cloudletList;
    /**
    * The container list
     */

    protected List<? extends Container> containerList;

    /**
     * The cloudlet submitted list.
     */
    protected List<? extends ContainerCloudlet> cloudletSubmittedList;

    /**
     * The cloudlet received list.
     */
    protected List<? extends ContainerCloudlet> cloudletReceivedList;

    /**
     * The cloudlets submitted.
     */
    protected int cloudletsSubmitted;

    /**
     * The vms requested.
     */
    protected int containerHostsRequested;

    /**
     * The vms acks.
     */
    protected int containerHostsAcks;
    /**
     * The containers acks.
     */
    protected int containersAcks;
    /**
     * The number of created containers
     */

    protected int containersCreated;

    /**
     * The vms destroyed.
     */
    protected int containerHostsDestroyed;

    /**
     * The datacenter ids list.
     */
    protected List<Integer> datacenterIdsList;

    /**
     * The datacenter requested ids list.
     */
    protected List<Integer> datacenterRequestedIdsList;

    /**
     * The vms to datacenters map.
     */
    protected Map<Integer, Integer> containerHostsToDatacentersMap;
 /**
     * The vms to datacenters map.
     */
    protected Map<Integer, Integer> containersToHostsMap;

    /**
     * The datacenter characteristics list.
     */
    protected Map<Integer, ContainerDatacenterCharacteristics> datacenterCharacteristicsList;

    /**
     * The datacenter characteristics list.
     */
    protected double overBookingfactor;

    protected int numberOfCreatedHOSTs;

    /**
     * Created a new DatacenterBroker object.
     *
     * @param name name to be associated with this entity (as required by Sim_entity class from
     *             simjava package)
     * @throws Exception the exception
     * @pre name != null
     * @post $none
     */
    public WFCScheduler(String name, double overBookingfactor) throws Exception {
        super(name);

        setContainerHostList(new ArrayList<ContainerHost>());
        setContainerList(new ArrayList<Container>());
        setContainerHostsCreatedList(new ArrayList<ContainerHost>());
        setContainersCreatedList(new ArrayList<Container>());
        setCloudletList(new ArrayList<ContainerCloudlet>());
        setCloudletSubmittedList(new ArrayList<ContainerCloudlet>());
        setCloudletReceivedList(new ArrayList<ContainerCloudlet>());
        cloudletsSubmitted = 0;
        setContainerHostsRequested(WFCConstants.WFC_NUMBER_HOSTS);
        setContainerHostsAcks(WFCConstants.WFC_NUMBER_HOSTS);
        setContainersAcks(0);
        setContainersCreated(WFCConstants.WFC_NUMBER_CONTAINER);
        setContainerHostsDestroyed(WFCConstants.WFC_NUMBER_HOSTS);
        setOverBookingfactor(overBookingfactor);
        setDatacenterIdsList(new LinkedList<Integer>());
        setDatacenterRequestedIdsList(new ArrayList<Integer>());
        setContainersToHostsMap(new HashMap<Integer, Integer>());
        setContainerHostsToDatacentersMap(new HashMap<Integer, Integer>());
        setDatacenterCharacteristicsList(new HashMap<Integer, ContainerDatacenterCharacteristics>());
        setNumberOfCreatedHOSTs(WFCConstants.WFC_NUMBER_HOSTS);
        
    }

    /**
     * This method is used to send to the broker the list with virtual machines that must be
     * created.
     *
     * @param list the list
     * @pre list !=null
     * @post $none
     */
    public void submitHostList(List<? extends ContainerHost> list) {
        getContainerHostList().addAll(list);
    }

    
    /**
     * Specifies that a given cloudlet must run in a specific virtual machine.
     *
     * @param cloudletId ID of the cloudlet being bount to a vm
     * @param containerId       the vm id
     * @pre cloudletId > 0
     * @pre id > 0
     * @post $none
     */
    
    /**
     * Specifies that a given cloudlet must run in a specific virtual machine.
     *
     * @param cloudletId ID of the cloudlet being bount to a vm
     * @param containerId       the vm id
     * @pre cloudletId > 0
     * @pre id > 0
     * @post $none
     */
    
    /*public void bindCloudletToContainer(int cloudletId, int containerId) {
        CloudletList.getById(getCloudletList(), cloudletId).setContainerId(containerId);
    }*/
    
    /**
     * Processes events available for this Broker.
     *
     * @param ev a SimEvent object
     * @pre ev != null
     * @post $none
     */
    
    
     /**
     * Process an event
     *
     * @param ev a simEvent obj
     */     
    @Override
    public void processEvent(SimEvent ev) {
        
        if(WFCConstants.CAN_PRINT_SEQ_LOG)
         Log.printLine("ContainerDataCenterBroker=WFScheduler=>ProccessEvent()=>ev.getTag():"+ev.getTag());       
        
        switch (ev.getTag()) {
            // Resource characteristics request
            case CloudSimTags.RESOURCE_CHARACTERISTICS_REQUEST:
                processResourceCharacteristicsRequest(ev);
                break;
            // Resource characteristics answer
            case CloudSimTags.RESOURCE_CHARACTERISTICS:
                processResourceCharacteristics(ev);
                break;
            // VM Creation answer
                /*
            case CloudSimTags.HOST_CREATE_ACK:
                processContainerHostCreate(ev);
                break;
            // New VM Creation answer
            case containerCloudSimTags.CONTAINER_NEW_CREATE:
                processNewContainerHostCreate(ev);
                break;            
                */
            case CloudSimTags.CLOUDLET_SUBMIT:
                processCloudletSubmit(ev);
                break;
            case WorkflowSimTags.CLOUDLET_CHECK:
                processCloudletReturn(ev);
                break;
            case WorkflowSimTags.CLOUDLET_UPDATE:
                processCloudletUpdate(ev);
                break;
            case CloudSimTags.CLOUDLET_RETURN:
                processCloudletReturn(ev);
                break;           
            case containerCloudSimTags.CONTAINER_CREATE_ACK:
                processContainerCreate(ev);
                break;
            case CloudSimTags.END_OF_SIMULATION:
                shutdownEntity();
                break;
            // other unknown tags are processed by this method
            default:
                processOtherEvent(ev);
                break;
        }
    }

    public void processContainerCreate(SimEvent ev) {
        int[] data = (int[]) ev.getData();
        int datacenterId = data[0];
        int hostId = data[1];
        int containerId = data[2];
        int result = data[3];

        if (result == CloudSimTags.TRUE) {
            if(hostId ==-1){
                Log.printConcatLine("Error : Where is the Host");}
            else{
                getContainersToHostsMap().put(containerId, hostId);
                getContainerHostsToDatacentersMap().put(hostId, datacenterId);

                if (ContainerList.getById(getContainerList(), containerId) != null) {

                    getContainerHostsCreatedList().add(ContainerList.getById(getContainerList(), containerId));
                    Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": The Container #", containerId, ", On Host#", hostId,", On Datacenter #",datacenterId);
                }
    //            ContainerVm p= ContainerList.getById(getContainerHostsCreatedList(), containerId);
                //int hostId = ContainerList.getById(getContainerHostsCreatedList(), hostId).getHost().getId();            
                //setContainersCreated(getContainersCreated()+1);}
           }
        }
        else {
            //Container container = ContainerList.getById(getContainerList(), containerId);
            Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Failed Creation of Container #", containerId,", On Datacenter #",datacenterId);
        }

        incrementContainersAcks();
        if (getContainersAcks() == getContainerList().size()) {
            //Log.print(getContainerHostsCreatedList().size() + "vs asli"+getContainerList().size());
            submitCloudlets();
            getContainerList().clear();
        }

    }

    /**
     * Process the return of a request for the characteristics of a PowerDatacenter.
     *
     * @param ev a SimEvent object
     * @pre ev != $null
     * @post $none
     */
    protected void processResourceCharacteristics(SimEvent ev) {
        ContainerDatacenterCharacteristics characteristics = (ContainerDatacenterCharacteristics) ev.getData();
        getDatacenterCharacteristicsList().put(characteristics.getId(), characteristics);

        if (getDatacenterCharacteristicsList().size() == getDatacenterIdsList().size()) {
            getDatacenterCharacteristicsList().clear();
            setDatacenterRequestedIdsList(new ArrayList<Integer>());
            submitContainers();
            //createContainerHostsInDatacenter(getDatacenterIdsList().get(0));
        }
    }

    /**
     * Process a request for the characteristics of a PowerDatacenter.
     *
     * @param ev a SimEvent object
     * @pre ev != $null
     * @post $none
     */
    protected void processResourceCharacteristicsRequest(SimEvent ev) {
        
        /*setDatacenterIdsList(CloudSim.getCloudResourceList());
        setDatacenterCharacteristicsList(new HashMap<Integer, ContainerDatacenterCharacteristics>());

        //Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Cloud Resource List received with ",
//                getDatacenterIdsList().size(), " resource(s)");

        for (Integer datacenterId : getDatacenterIdsList()) {
            sendNow(datacenterId, CloudSimTags.RESOURCE_CHARACTERISTICS, getId());
        }
        
        */
        
          setDatacenterCharacteristicsList(new HashMap<>());
        Log.printLine(CloudSim.clock() + ": " + getName() + ": Cloud Resource List received with "
                + getDatacenterIdsList().size() + " resource(s)");
        for (Integer datacenterId : getDatacenterIdsList()) {
            sendNow(datacenterId, CloudSimTags.RESOURCE_CHARACTERISTICS, getId());
        }
    }
    
    /*
    protected void processNewContainerHostCreate(SimEvent ev) {
        Map<String, Object> map = (Map<String, Object>) ev.getData();
        int datacenterId = (int) map.get("datacenterID");
        int result = (int) map.get("result");
        Container container = (Container) map.get("container");
        int containerId = container.getId();
        if (result == CloudSimTags.TRUE) {
            getContainerList().add(container);
            getContainerHostsToDatacentersMap().put(containerId, datacenterId);
            getContainerHostsCreatedList().add(container);
            Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Container #", containerId,
                    " has been created in Datacenter #", datacenterId, ", Host #",
                    ContainerList.getById(getContainerHostsCreatedList(), containerId).getHost().getId());
        } else {
            Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Creation of CONTAINER #", containerId,
                    " failed in Datacenter #", datacenterId);
        }
    }
    */
    /**
     * Process the ack received due to a request for VM creation.
     *
     * @param ev a SimEvent object
     * @pre ev != null
     * @post $none
     */
  /*  protected void processContainerHostCreate(SimEvent ev) {
        
        int[] data = (int[]) ev.getData();
        int datacenterId = data[0];
        int hostId = data[1];
        int result = data[2];

        if (result == CloudSimTags.TRUE) {
            getContainerHostsToDatacentersMap().put(hostId, datacenterId);
            getContainerHostsCreatedList().add(ContainerList.getById(getContainerList(), hostId));
            Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": HOST #", hostId,
                    " has been created in Datacenter #", datacenterId, ", Host #",
                    ContainerList.getById(getContainerHostsCreatedList(), hostId).getHost().getId());
                        setNumberOfCreatedHOSTs(getNumberOfCreatedHOSTs()+1);
        } else {
            Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Creation of HOST #", hostId,
                    " failed in Datacenter #", datacenterId);
        }

        incrementContainerHostsAcks();
//        if (getContainerHostsCreatedList().size() == getVmList().size() - getVmsDestroyed()) {
//        If we have tried creating all of the vms in the data center, we submit the containers.
        if(getContainerHostList().size() == containerHostsAcks){
            
            submitContainers();
        }
    }
*/
    
    protected void submitContainers(){      
        sendNow(getDatacenterIdsList().get(0), containerCloudSimTags.CONTAINER_SUBMIT,getContainerList());
    }

    
    /**
     * Process a cloudlet return event.
     *
     * @param ev a SimEvent object
     * @pre ev != $null
     * @post $none
     */
    protected void processCloudletReturn(SimEvent ev) {
        ContainerCloudlet cloudlet = (ContainerCloudlet) ev.getData();
        Job job = (Job) cloudlet;
        
        if(WFCConstants.FAILURE_FLAG)
          FailureGenerator.generate(job);
        
        getCloudletReceivedList().add(cloudlet);
        getCloudletSubmittedList().remove(cloudlet);//
        
        Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Cloudlet ", cloudlet.getCloudletId()," returned");
        Log.printConcatLine(CloudSim.clock(), ": ", getName(), "The number of finished Cloudlets is:", getCloudletReceivedList().size());
        //cloudletsSubmitted--;
        
        /*if (getCloudletList().size() == 0 && cloudletsSubmitted == 0) { // all cloudlets executed
            Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": All Cloudlets executed. Finishing...");
            clearDatacenters();
            finishExecution();
        } else { // some cloudlets haven't finished yet
            if (getCloudletList().size() > 0 && cloudletsSubmitted == 0) {
                // all the cloudlets sent finished. It means that some bount
                // cloudlet is waiting its VM be created
                clearDatacenters();
                createVmsInDatacenter(0);
            }

        } */
        //*from wfScheduler                
        
        
        //ContainerVm vm = (ContainerVm) getContainerHostsCreatedList().get(cloudlet.getVmId());
        Container container= ContainerList.getById(getContainerHostsCreatedList(), cloudlet.getContainerId());
        //so that this resource is released
        //ToDo
        container.setState(WorkflowSimTags.VM_STATUS_IDLE);

        double delay = 0.0;
        if (Parameters.getOverheadParams().getPostDelay() != null) {
            delay = Parameters.getOverheadParams().getPostDelay(job);
        }
        schedule(this.workflowEngineId, delay, CloudSimTags.CLOUDLET_RETURN, cloudlet);

        cloudletsSubmitted--;
        //not really update right now, should wait 1 s until many jobs have returned
        schedule(this.getId(), 0.0, WorkflowSimTags.CLOUDLET_UPDATE);
        //*
    }

    /**
     * Overrides this method when making a new and different type of Broker. This method is called
     * by  for incoming unknown tags.
     *
     * @param ev a SimEvent object
     * @pre ev != null
     * @post $none
     */
    protected void processOtherEvent(SimEvent ev) {
        if (ev == null) {
            Log.printConcatLine(getName(), ".processOtherEvent(): ", "Error - an event is null.");
            return;
        }

        Log.printConcatLine(getName(), ".processOtherEvent(): Error - event unknown by this DatacenterBroker.");
    }

    /**
     * Create the virtual machines in a datacenter.
     *
     * @param datacenterId Id of the chosen PowerDatacenter
     * @pre $none
     * @post $none
     */
    protected void createContainerHostsInDatacenter(int datacenterId) {
        // send as much hosts as possible for this datacenter before trying the next one
        int requestedHosts = 0;
        String datacenterName = CloudSim.getEntityName(datacenterId);
        for (ContainerHost host : getContainerHostList()) {
            if (!getContainerHostsToDatacentersMap().containsKey(host.getId())) {
                Log.printLine(String.format("%s: %s: Trying to Create Host #%d in %s", CloudSim.clock(), getName(), host.getId(), datacenterName));
                sendNow(datacenterId, CloudSimTags.HOST_CREATE_ACK, host);
                requestedHosts++;
            }
        }

        getDatacenterRequestedIdsList().add(datacenterId);

        setContainerHostsRequested(requestedHosts);
        setContainerHostsAcks(0);
    }



    /**getOverBookingfactor
     * Destroy the virtual machines running in datacenters.
     *
     * @pre $none
     * @post $none
     */
    protected void clearDatacenters() {
        for (Container container : getContainerHostsCreatedList()) {
//            Log.printConcatLine(CloudSim.clock(), ": " + getName(), ": Destroying container #", container.getId());
            sendNow(getContainerHostsToDatacentersMap().get(container.getId()), CloudSimTags.HOST_DESTROY, container);
        }

        getContainerHostsCreatedList().clear();
    }


    /**
     *
     */
    


    /**
     * Send an internal event communicating the end of the simulation.
     *
     * @pre $none
     * @post $none
     */
    protected void finishExecution() {
        sendNow(getId(), CloudSimTags.END_OF_SIMULATION);
    }

    /*
     * (non-Javadoc)
     * @see cloudsim.core.SimEntity#shutdownEntity()
     */
    @Override
    public void shutdownEntity() {
       // clearDatacenters();//added
        Log.printConcatLine(getName(), " is shutting down...");
    }

    /*
     * (non-Javadoc)
     * @see cloudsim.core.SimEntity#startEntity()
     */
    @Override
    public void startEntity() {
        Log.printConcatLine(getName(), " is starting...");
        //schedule(getId(), 0, CloudSimTags.RESOURCE_CHARACTERISTICS_REQUEST);
         int gisID = -1;
        if (gisID == -1) {
            gisID = CloudSim.getCloudInfoServiceEntityId();
        }

        // send the registration to GIS
        sendNow(gisID, CloudSimTags.REGISTER_RESOURCE, getId());
    }

    /**
     * Gets the container list.
     *
     * @param <T> the generic type
     * @return the container list
     */
    @SuppressWarnings("unchecked")
    public <T extends ContainerHost> List<T> getContainerHostList() {
        return (List<T>) containerHostList;
    }

    /**
     * Sets the container list.
     *
     * @param <T>    the generic type
     * @param containerList the new container list
     */
    protected <T extends ContainerHost> void setContainerHostList(List<T> containerHostList) {
        this.containerHostList = containerHostList;
    }

    /**
     * Gets the cloudlet list.
     *
     * @param <T> the generic type
     * @return the cloudlet list
     */
    //@SuppressWarnings("unchecked")
    public <T extends ContainerCloudlet> List<T> getCloudletList() {
        return (List<T>) cloudletList;
    }

    /**
     * Sets the cloudlet list.
     *
     * @param <T>          the generic type
     * @param cloudletList the new cloudlet list
     */
    protected <T extends ContainerCloudlet> void setCloudletList(List<T> cloudletList) {
        this.cloudletList = cloudletList;
    }

    /**
     * Gets the cloudlet submitted list.
     *
     * @param <T> the generic type
     * @return the cloudlet submitted list
     */
    @SuppressWarnings("unchecked")
    public <T extends ContainerCloudlet> List<T> getCloudletSubmittedList() {
        return (List<T>) cloudletSubmittedList;
    }

    /**
     * Sets the cloudlet submitted list.
     *
     * @param <T>                   the generic type
     * @param cloudletSubmittedList the new cloudlet submitted list
     */
    protected <T extends ContainerCloudlet> void setCloudletSubmittedList(List<T> cloudletSubmittedList) {
        this.cloudletSubmittedList = cloudletSubmittedList;
    }

    /**
     * Gets the cloudlet received list.
     *
     * @param <T> the generic type
     * @return the cloudlet received list
     */
    @SuppressWarnings("unchecked")
    public <T extends ContainerCloudlet> List<T> getCloudletReceivedList() {
        return (List<T>) cloudletReceivedList;
    }

    /**
     * Sets the cloudlet received list.
     *
     * @param <T>                  the generic type
     * @param cloudletReceivedList the new cloudlet received list
     */
    protected <T extends ContainerCloudlet> void setCloudletReceivedList(List<T> cloudletReceivedList) {
        this.cloudletReceivedList = cloudletReceivedList;
    }

    /**
     * Gets the container list.
     *
     * @param <T> the generic type
     * @return the container list
     */
    @SuppressWarnings("unchecked")
    public <T extends Container> List<T> getContainerHostsCreatedList() {
        return (List<T>) containersCreatedList;
    }

    /**
     * Sets the container list.
     *
     * @param <T>            the generic type
     * @param vmsCreatedList the vms created list
     */
    protected <T extends ContainerHost> void setContainerHostsCreatedList(List<T> ContainerHostsCreatedList) {
        this.containerHostsCreatedList = containerHostsCreatedList;
    }

    /**
     * Gets the vms requested.
     *
     * @return the vms requested
     */
    protected int getHostsRequested() {
        return containerHostsRequested;
    }

    /**
     * Sets the vms requested.
     *
     * @param vmsRequested the new vms requested
     */
    protected void setContainerHostsRequested(int containersRequested) {
        this.containerHostsRequested = containerHostsRequested;
    }

    /**
     * Gets the vms acks.
     *
     * @return the vms acks
     */
    protected int getContainerHostsAcks() {
        return containerHostsAcks;
    }

    /**
     * Sets the vms acks.
     *
     * @param vmsAcks the new vms acks
     */
    protected void setContainerHostsAcks(int hostsAcks) {
        this.containerHostsAcks = hostsAcks;
    }

    /**
     * Increment vms acks.
     */
    protected void incrementContainerHostsAcks() {
        containerHostsAcks++;
    }
    /**
     * Increment vms acks.
     */
    protected void incrementContainersAcks() {
        setContainersAcks(getContainersAcks()+1);
    }

    /**
     * Gets the vms destroyed.
     *
     * @return the vms destroyed
     */
    protected int getContainerHostsDestroyed() {
        return containerHostsDestroyed;
    }

    /**
     * Sets the vms destroyed.
     *
     * @param vmsDestroyed the new vms destroyed
     */
    protected void setContainerHostsDestroyed(int hostsDestroyed) {
        this.containerHostsDestroyed = hostsDestroyed;
    }

    /**
     * Gets the datacenter ids list.
     *
     * @return the datacenter ids list
     */
    protected List<Integer> getDatacenterIdsList() {
        return datacenterIdsList;
    }

    /**
     * Sets the datacenter ids list.
     *
     * @param datacenterIdsList the new datacenter ids list
     */
    protected void setDatacenterIdsList(List<Integer> datacenterIdsList) {
        this.datacenterIdsList = datacenterIdsList;
    }

   
    /**
     * Gets the datacenter characteristics list.
     *
     * @return the datacenter characteristics list
     */
    protected Map<Integer, ContainerDatacenterCharacteristics> getDatacenterCharacteristicsList() {
        return datacenterCharacteristicsList;
    }

    /**
     * Sets the datacenter characteristics list.
     *
     * @param datacenterCharacteristicsList the datacenter characteristics list
     */
    protected void setDatacenterCharacteristicsList(
            Map<Integer, ContainerDatacenterCharacteristics> datacenterCharacteristicsList) {
        this.datacenterCharacteristicsList = datacenterCharacteristicsList;
    }

    /**
     * Gets the datacenter requested ids list.
     *
     * @return the datacenter requested ids list
     */
    protected List<Integer> getDatacenterRequestedIdsList() {
        return datacenterRequestedIdsList;
    }

    /**
     * Sets the datacenter requested ids list.
     *
     * @param datacenterRequestedIdsList the new datacenter requested ids list
     */
    protected void setDatacenterRequestedIdsList(List<Integer> datacenterRequestedIdsList) {
        this.datacenterRequestedIdsList = datacenterRequestedIdsList;
    }

//------------------------------------------------

    public <T extends Container> List<T> getContainerList() {
        return (List<T>) containerList;
    }

    public void setContainerList(List<? extends Container> containerList) {
        this.containerList = containerList;
    }
    /**
     * This method is used to send to the broker the list with virtual machines that must be
     * created.
     *
     * @param list the list
     * @pre list !=null
     * @post $none
     */
    public void submitContainerList(List<? extends Container> list) {
        getContainerList().addAll(list);
    }


    public Map<Integer, Integer> getContainersToHostsMap() {
        return containersToHostsMap;
    }

    public void setContainersToHostsMap(Map<Integer, Integer> containersToHostsMap) {
        this.containersToHostsMap = containersToHostsMap;
    }

  
     /**
     * Gets the vms to datacenters map.
     *
     * @return the vms to datacenters map
     */
    protected Map<Integer, Integer> getContainerHostsToDatacentersMap() {
        return containerHostsToDatacentersMap;
    }
   
    /**
     * Sets the vms to datacenters map.
     *
     * @param vmsToDatacentersMap the vms to datacenters map
     */
    protected void setContainerHostsToDatacentersMap(Map<Integer, Integer> hostsToDatacentersMap) {
        this.containerHostsToDatacentersMap = hostsToDatacentersMap;
    }


    public void setContainersCreatedList(List<? extends Container> containersCreatedList) {
        this.containersCreatedList = containersCreatedList;
    }

    public int getContainersAcks() {
        return containersAcks;
    }

    public void setContainersAcks(int containersAcks) {
        this.containersAcks = containersAcks;
    }

    public int getContainersCreated() {
        return containersCreated;
    }

    public void setContainersCreated(int containersCreated) {
        this.containersCreated = containersCreated;
    }

    public double getOverBookingfactor() {
        return overBookingfactor;
    }

    public void setOverBookingfactor(double overBookingfactor) {
        this.overBookingfactor = overBookingfactor;
    }

    public int getNumberOfCreatedHOSTs() {
        return numberOfCreatedHOSTs;
    }

    public void setNumberOfCreatedHOSTs(int numberOfCreatedHOSTs) {
        this.numberOfCreatedHOSTs = numberOfCreatedHOSTs;
    }
    
    
    
    
    
    
    
     /**
     * Binds this scheduler to a datacenter
     *
     * @param datacenterId data center id
     */
    public void bindSchedulerDatacenter(int datacenterId) {
        if (datacenterId <= 0) {
            Log.printLine("Error in data center id");
            return;
        }
        this.datacenterIdsList.add(datacenterId);
    }

    /**
     * Sets the workflow engine id
     *
     * @param workflowEngineId the workflow engine id
     */
    public void setWorkflowEngineId(int workflowEngineId) {
        this.workflowEngineId = workflowEngineId;
    }

   

    /**
     * Switch between multiple schedulers. Based on algorithm.method
     *
     * @param name the SchedulingAlgorithm name
     * @return the algorithm that extends BaseSchedulingAlgorithm
     */
    private BaseSchedulingAlgorithm getScheduler(Parameters.SchedulingAlgorithm name) {
        BaseSchedulingAlgorithm algorithm;

        // choose which algorithm to use. Make sure you have add related enum in
        //Parameters.java
        switch (name) {
            //by default it is Static
            case FCFS:
                algorithm = new FCFSSchedulingAlgorithm();
                break;
            case MINMIN:
                algorithm = new MinMinSchedulingAlgorithm();
                break;
            case MAXMIN:
                algorithm = new MaxMinSchedulingAlgorithm();
                break;
            case MCT:
                algorithm = new MCTSchedulingAlgorithm();
                break;
            case DATA:
                algorithm = new DataAwareSchedulingAlgorithm();
                break;
            case STATIC:
                algorithm = new StaticSchedulingAlgorithm();
                break;
            case ROUNDROBIN:
                algorithm = new RoundRobinSchedulingAlgorithm();
                break;
            default:
                algorithm = new StaticSchedulingAlgorithm();
                break;

        }
        return algorithm;
    }

    
    
    /**
     * Update a cloudlet (job)
     *
     * @param ev a simEvent object
     */
    protected void processCloudletUpdate(SimEvent ev) {

        List<ContainerCloudlet> scheduledList = getCloudletList();//scheduler.getScheduledList();
        
        /*BaseSchedulingAlgorithm scheduler = getScheduler(Parameters.getSchedulingAlgorithm());
        scheduler.setCloudletList(scheduledList);
       scheduler.setVmList(getContainerHostsCreatedList());

        try {
           scheduler.run();
        } catch (Exception e) {
           
            Log.printLine("Error in configuring scheduler_method");
            Log.printLine(e.getMessage());
            //by arman I commented log  e.printStackTrace();
        }
        */
               
        int containerIndex = 0;
        List<ContainerCloudlet> successfullySubmitted = new ArrayList<>();

        for (ContainerCloudlet cloudlet : scheduledList) {

            if (containerIndex < getContainersCreated()) {                            
                if(getContainersToHostsMap().get(cloudlet.getCloudletId()) != null) {
                    int containerId = getContainersToHostsMap().get(cloudlet.getCloudletId());
                    cloudlet.setHostId(containerId);
                    cloudlet.setContainerId(cloudlet.getCloudletId());
                    
                    containerIndex++;
                    //sendNow(getDatacenterIdsList().get(0), CloudSimTags.CLOUDLET_SUBMIT, cloudlet);
                    cloudletsSubmitted++;
                    //getCloudletSubmittedList().add(cloudlet);
                    
                   
                    // int containerId= getContainersToVmsMap().get(cloudlet.getContainerId());
                     double delay = 0.0;
                     if (Parameters.getOverheadParams().getQueueDelay() != null) {
                         delay = Parameters.getOverheadParams().getQueueDelay(cloudlet);
                     }
                     schedule(getContainerHostsToDatacentersMap().get(containerId), delay, CloudSimTags.CLOUDLET_SUBMIT, cloudlet);

                    successfullySubmitted.add(cloudlet);
                }
            }
        }

        //getCloudletList().removeAll(successfullySubmitted);
        //successfullySubmitted.clear();
        
        //List<ContainerCloudlet> scheduledList = successfullySubmitted;
        
        /*for (ContainerCloudlet cloudlet : successfullySubmitted) {
            int containerId = cloudlet.getVmId();
           // int containerId= getContainersToVmsMap().get(cloudlet.getContainerId());
            double delay = 0.0;
            if (Parameters.getOverheadParams().getQueueDelay() != null) {
                delay = Parameters.getOverheadParams().getQueueDelay(cloudlet);
            }
            schedule(getVmsToDatacentersMap().get(containerId), delay, CloudSimTags.CLOUDLET_SUBMIT, cloudlet);
        }*/
        getCloudletList().removeAll(scheduledList);
        getCloudletSubmittedList().addAll(scheduledList);
        //cloudletsSubmitted += scheduledList.size();  
    }

  
    /**
     * A trick here. Assure that we just submit it once
     */
    private boolean processCloudletSubmitHasShown = false;

    /**
     * Submits cloudlet (job) list
     *
     * @param ev a simEvent object
     */
    protected void processCloudletSubmit(SimEvent ev) {
        List<Job> list = (List) ev.getData();
        getCloudletList().addAll(list);

        sendNow(this.getId(), WorkflowSimTags.CLOUDLET_UPDATE);
         if (!processCloudletSubmitHasShown) {
            processCloudletSubmitHasShown = true;
        }
    }
    
    /**
     * Submit cloudlets to the created VMs.
     *
     * @pre $none
     * @post $none
     */
    protected void submitCloudlets() {    
       
      //sendNow(getDatacenterIdsList().get(0), WorkflowSimTags.CLOUDLET_UPDATE, null);
     sendNow(this.workflowEngineId, CloudSimTags.CLOUDLET_SUBMIT, null);
     
     
    
    }
 

}


