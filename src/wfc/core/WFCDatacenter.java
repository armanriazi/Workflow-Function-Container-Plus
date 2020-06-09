package wfc.core;


import org.cloudbus.cloudsim.container.resourceAllocators.ContainerAllocationPolicy;
import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.core.ContainerCloudlet;
import org.cloudbus.cloudsim.container.core.ContainerDatacenterCharacteristics;
import org.cloudbus.cloudsim.container.core.ContainerHost;
import org.cloudbus.cloudsim.container.core.containerCloudSimTags;
import wfc.resourceAllocators.ExContainerAllocationPolicy;
import org.cloudbus.cloudsim.container.schedulers.ContainerCloudletScheduler;
import org.workflowsim.FileItem;
import org.workflowsim.Job;
import org.workflowsim.Task;
import org.workflowsim.utils.Parameters;
import org.workflowsim.utils.Parameters.ClassType;
import org.workflowsim.utils.WFCReplicaCatalog;

/**
 * Created by sareh on 10/07/15.
 */
public class WFCDatacenter extends SimEntity {

    /**
     * The characteristics.
     */
    private ContainerDatacenterCharacteristics characteristics;

    /**
     * The regional cis name.
     */
    private String regionalCisName;

  
    /**
     * The container provisioner.
     */
    private ContainerAllocationPolicy containerAllocationPolicy;

    /**
     * The last process time.
     */
    private double lastProcessTime;

    /**
     * The storage list.
     */
    private List<Storage> storageList;

    /**
     * The host list.
     */
    private List<? extends ContainerHost> containerHostList;
    /**
     * The container list.
     */
    private List<? extends Container> containerList;

    /**
     * The scheduling interval.
     */
    private double schedulingInterval;
    /**
     * The scheduling interval.
     */
    private String experimentName;
    /**
     * The log address.
     */
    private String logAddress;


    /**
     * Allocates a new PowerDatacenter object.
     * @param name
     * @param characteristics
     * @param hostAllocationPolicy
     * @param containerAllocationPolicy
     * @param storageList
     * @param schedulingInterval
     * @param experimentName
     * @param logAddress
     * @throws Exception
     */
    public WFCDatacenter(
            String name,
            ContainerDatacenterCharacteristics characteristics,   
            List<ContainerHost> hostList,                                                                  
            ContainerAllocationPolicy containerAllocationPolicy,
            List<Storage> storageList,
            String experimentName,
            double schedulingInterval, String logAddress,double containerStartupDelay) throws Exception {
        super(name);

        setCharacteristics(characteristics);        
        setContainerAllocationPolicy(containerAllocationPolicy);
        setLastProcessTime(0.0);
        setStorageList(storageList);
        setContainerHostList(hostList);
        setContainerList(new ArrayList<Container>());
        setSchedulingInterval(schedulingInterval);
        setExperimentName(experimentName);
        setLogAddress(logAddress);
        
        for (ContainerHost host : getCharacteristics().getHostList()) {
            host.setDatacenter(this);
        }

        // If this resource doesn't have any PEs then no useful at all
        if (getCharacteristics().getNumberOfPes() == 0) {
            throw new Exception(super.getName()
                    + " : Error - this entity has no PEs. Therefore, can't process any Cloudlets.");
        }

        // stores id of this class
        getCharacteristics().setId(super.getId());
    }

    /**
     * Overrides this method when making a new and different type of resource. <br>
     * <b>NOTE:</b> You do not need to override {} method, if you use this method.
     *
     * @pre $none
     * @post $none
     */
    protected void registerOtherEntity() {
        // empty. This should be override by a child class
    }

    /**
     * Processes events or services that are available for this PowerDatacenter.
     *
     * @param ev a Sim_event object
     * @pre ev != null
     * @post $none
     */
    @Override
    public void processEvent(SimEvent ev) {
        
        int srcId = -1;
        if(WFCConstants.CAN_PRINT_SEQ_LOG)
        Log.printLine("WFCDataCener=>ProccessEvent()=>ev.getTag():"+ev.getTag());
        
        switch (ev.getTag()) {
            // Resource characteristics inquiry
            case CloudSimTags.RESOURCE_CHARACTERISTICS:
                srcId = ((Integer) ev.getData()).intValue();
                sendNow(srcId, ev.getTag(), getCharacteristics());
                break;

            // Resource dynamic info inquiry
            case CloudSimTags.RESOURCE_DYNAMICS:
                srcId = ((Integer) ev.getData()).intValue();
                sendNow(srcId, ev.getTag(), 0);
                break;

            case CloudSimTags.RESOURCE_NUM_PE:
                srcId = ((Integer) ev.getData()).intValue();
                int numPE = getCharacteristics().getNumberOfPes();
                sendNow(srcId, ev.getTag(), numPE);
                break;

            case CloudSimTags.RESOURCE_NUM_FREE_PE:
                srcId = ((Integer) ev.getData()).intValue();
                int freePesNumber = getCharacteristics().getNumberOfFreePes();
                sendNow(srcId, ev.getTag(), freePesNumber);
                break;

            // New Cloudlet arrives
            case CloudSimTags.CLOUDLET_SUBMIT:
                processCloudletSubmit(ev, false);
                break;

            // New Cloudlet arrives, but the sender asks for an ack
            case CloudSimTags.CLOUDLET_SUBMIT_ACK:
                processCloudletSubmit(ev, true);
                break;

            // Cancels a previously submitted Cloudlet
            case CloudSimTags.CLOUDLET_CANCEL:
                processCloudlet(ev, CloudSimTags.CLOUDLET_CANCEL);
                break;

            // Pauses a previously submitted Cloudlet
            case CloudSimTags.CLOUDLET_PAUSE:
                processCloudlet(ev, CloudSimTags.CLOUDLET_PAUSE);
                break;

            // Pauses a previously submitted Cloudlet, but the sender
            // asks for an acknowledgement
            case CloudSimTags.CLOUDLET_PAUSE_ACK:
                processCloudlet(ev, CloudSimTags.CLOUDLET_PAUSE_ACK);
                break;

            // Resumes a previously submitted Cloudlet
            case CloudSimTags.CLOUDLET_RESUME:
                processCloudlet(ev, CloudSimTags.CLOUDLET_RESUME);
                break;

            // Resumes a previously submitted Cloudlet, but the sender
            // asks for an acknowledgement
            case CloudSimTags.CLOUDLET_RESUME_ACK:
                processCloudlet(ev, CloudSimTags.CLOUDLET_RESUME_ACK);
                break;

            // Moves a previously submitted Cloudlet to a different resource
            case CloudSimTags.CLOUDLET_MOVE:
                processCloudletMove((int[]) ev.getData(), CloudSimTags.CLOUDLET_MOVE);
                break;

            // Moves a previously submitted Cloudlet to a different resource
            case CloudSimTags.CLOUDLET_MOVE_ACK:
                processCloudletMove((int[]) ev.getData(), CloudSimTags.CLOUDLET_MOVE_ACK);
                break;

            // Checks the status of a Cloudlet
            case CloudSimTags.CLOUDLET_STATUS:
                processCloudletStatus(ev);
                break;

            // Ping packet
            case CloudSimTags.INFOPKT_SUBMIT:
                processPingRequest(ev);
                break;
/*
            case CloudSimTags.HOST_CREATE:
                processHostCreate(ev, false);
                break;

            case CloudSimTags.HOST_CREATE_ACK:
                processHostCreate(ev, true);
                break;

            case CloudSimTags.HOST_DESTROY:
                processHostDestroy(ev, false);
                break;

            case CloudSimTags.HOST_DESTROY_ACK:
                processHostDestroy(ev, true);
                break;

            case CloudSimTags.HOST_MIGRATE:
                processHostMigrate(ev, false);
                break;

            case CloudSimTags.HOST_MIGRATE_ACK:
                processHostMigrate(ev, true);
                break;
 */
            case CloudSimTags.HOST_DATA_ADD:
                processDataAdd(ev, false);
                break;

            case CloudSimTags.HOST_DATA_ADD_ACK:
                processDataAdd(ev, true);
                break;

            case CloudSimTags.HOST_DATA_DEL:
                processDataDelete(ev, false);
                break;

            case CloudSimTags.HOST_DATA_DEL_ACK:
                processDataDelete(ev, true);
                break;
           
            case CloudSimTags.HOST_DATACENTER_EVENT:
                updateCloudletProcessing();
                checkCloudletCompletion();
                break;
               
            case containerCloudSimTags.CONTAINER_SUBMIT:
                processContainerSubmit(ev, true);
                break;

            case containerCloudSimTags.CONTAINER_MIGRATE:
                processContainerMigrate(ev, false);
                // other unknown tags are processed by this method
                break;
            case CloudSimTags.END_OF_SIMULATION:
                shutdownEntity();
                break;
            default:
                processOtherEvent(ev);
                break;
        }
    }

    public void processContainerSubmit(SimEvent ev, boolean ack) {
        List<Container> containerList = (List<Container>) ev.getData();

        for (Container container : containerList) {
            boolean result = getContainerAllocationPolicy().allocateHostForContainer(container, getContainerHostList());
            if (ack) {
                int[] data = new int[4];
                data[2] = container.getId();
                if (result) {
                    data[3] = CloudSimTags.TRUE;
                } else {
                    data[3] = CloudSimTags.FALSE;
                }
                if (result) {
                    ContainerHost containerHost = getContainerAllocationPolicy().getContainerHostList().get(0);
                    data[1] = containerHost.getId();
                    data[0] = this.getId();
                    if(containerHost.getId() == -1){

                        Log.printConcatLine("The ContainerHOST ID is not known (-1) !");
                    }
                    
                    
                    Log.printConcatLine("Assigning the container#" + container.getUid() + "to HOST #" + containerHost.getId() + "to Datacenter #" + this.getId());
                    
                    getContainerList().add(container);
                    if (container.isBeingInstantiated()) {
                        container.setBeingInstantiated(false);
                    }
                    container.updateContainerProcessing(CloudSim.clock(), getContainerAllocationPolicy().getContainerHost(container).getContainerScheduler().getAllocatedMipsForContainer(container));
                } else {
                    data[0] = -1;
                    //notAssigned.add(container);
                    Log.printLine(String.format("Couldn't find a host to host the container #%s", container.getUid()));

                }
                
                send(ev.getSource(), CloudSim.getMinTimeBetweenEvents(), containerCloudSimTags.CONTAINER_CREATE_ACK, data);

            }
        }

    }

    
    
 
    /**
     * Process data del.
     *
     * @param ev  the ev
     * @param ack the ack
     */
    protected void processDataDelete(SimEvent ev, boolean ack) {
        if (ev == null) {
            return;
        }

        Object[] data = (Object[]) ev.getData();
        if (data == null) {
            return;
        }

        String filename = (String) data[0];
        int req_source = ((Integer) data[1]).intValue();
        int tag = -1;

        // check if this file can be deleted (do not delete is right now)
        int msg = deleteFileFromStorage(filename);
        if (msg == DataCloudTags.FILE_DELETE_SUCCESSFUL) {
            tag = DataCloudTags.CTLG_DELETE_MASTER;
        } else { // if an error occured, notify user
            tag = DataCloudTags.FILE_DELETE_MASTER_RESULT;
        }

        if (ack) {
            // send back to sender
            Object pack[] = new Object[2];
            pack[0] = filename;
            pack[1] = Integer.valueOf(msg);

            sendNow(req_source, tag, pack);
        }
    }

    /**
     * Process data add.
     *
     * @param ev  the ev
     * @param ack the ack
     */
    protected void processDataAdd(SimEvent ev, boolean ack) {
        if (ev == null) {
            return;
        }

        Object[] pack = (Object[]) ev.getData();
        if (pack == null) {
            return;
        }

        File file = (File) pack[0]; // get the file
        file.setMasterCopy(true); // set the file into a master copy
        int sentFrom = ((Integer) pack[1]).intValue(); // get sender ID

        /******
         * // DEBUG Log.printLine(super.get_name() + ".addMasterFile(): " + file.getName() +
         * " from " + CloudSim.getEntityName(sentFrom));
         *******/

        Object[] data = new Object[3];
        data[0] = file.getName();

        int msg = addFile(file); // add the file

        if (ack) {
            data[1] = Integer.valueOf(-1); // no sender id
            data[2] = Integer.valueOf(msg); // the result of adding a master file
            sendNow(sentFrom, DataCloudTags.FILE_ADD_MASTER_RESULT, data);
        }
    }

    /**
     * Processes a ping request.
     *
     * @param ev a Sim_event object
     * @pre ev != null
     * @post $none
     */
    protected void processPingRequest(SimEvent ev) {
        InfoPacket pkt = (InfoPacket) ev.getData();
        pkt.setTag(CloudSimTags.INFOPKT_RETURN);
        pkt.setDestId(pkt.getSrcId());

        // sends back to the sender
        sendNow(pkt.getSrcId(), CloudSimTags.INFOPKT_RETURN, pkt);
    }

    /**
     * Process the event for an User/Broker who wants to know the status of a Cloudlet. This
     * PowerDatacenter will then send the status back to the User/Broker.
     *
     * @param ev a Sim_event object
     * @pre ev != null
     * @post $none
     */
    protected void processCloudletStatus(SimEvent ev) {
        int cloudletId = 0;
        int userId = 0;
        int hostId = 0;
        int containerId = 0;
        int status = -1;

        try {
            // if a sender using cloudletXXX() methods
            int data[] = (int[]) ev.getData();
            cloudletId = data[0];
            userId = data[1];
            hostId = data[2];
            containerId = data[3];
            //Log.printLine("Data Center is processing the cloudletStatus Event ");
            status = getContainerAllocationPolicy().getContainerHost(hostId, userId).
                    getContainer(containerId, userId).getContainerCloudletScheduler().getCloudletStatus(cloudletId);
        }

        // if a sender using normal send() methods
        catch (ClassCastException c) {
            try {
                ContainerCloudlet cl = (ContainerCloudlet) ev.getData();
                cloudletId = cl.getCloudletId();
                userId = cl.getUserId();
                containerId = cl.getContainerId();

                status = getContainerAllocationPolicy().getContainerHost(hostId, userId).getContainer(containerId, userId)
                        .getContainerCloudletScheduler().getCloudletStatus(cloudletId);
            } catch (Exception e) {
                Log.printConcatLine(getName(), ": Error in processing CloudSimTags.CLOUDLET_STATUS");
                Log.printLine(e.getMessage());
                return;
            }
        } catch (Exception e) {
            Log.printConcatLine(getName(), ": Error in processing CloudSimTags.CLOUDLET_STATUS");
            Log.printLine(e.getMessage());
            return;
        }

        int[] array = new int[3];
        array[0] = getId();
        array[1] = cloudletId;
        array[2] = status;

        int tag = CloudSimTags.CLOUDLET_STATUS;
        sendNow(userId, tag, array);
    }

    /**
     * Here all the method related to HOST requests will be received and forwarded to the related
     * method.
     *
     * @param ev the received event
     * @pre $none
     * @post $none
     */
    protected void processOtherEvent(SimEvent ev) {
        if (ev == null) {
            Log.printConcatLine(getName(), ".processOtherEvent(): Error - an event is null.");
        }
    }

    /**
     * Process the event for a User/Broker who wants to create a HOST in this PowerDatacenter. This
     * PowerDatacenter will then send the status back to the User/Broker.
     *
     * @param ev  a Sim_event object
     * @param ack the ack
     * @pre ev != null
     * @post $none
     */
    /*
    protected void processHostCreate(SimEvent ev, boolean ack) {
        ContainerHost containerHost = (ContainerHost) ev.getData();

        boolean result = getContainerAllocationPolicy().allocateHostForContainer(containerHost, getContainerHostList());

        if (ack) {
            int[] data = new int[3];
            data[0] = getId();
            data[1] = containerHost.getId();

            if (result) {
                data[2] = CloudSimTags.TRUE;
            } else {
                data[2] = CloudSimTags.FALSE;
            }
            send(containerHost.getUserId(), CloudSim.getMinTimeBetweenEvents(), CloudSimTags.HOST_CREATE_ACK, data);
        }

        if (result) {
            getContainerList().add(containerHost);

            if (containerHost.isBeingInstantiated()) {
                containerHost.setBeingInstantiated(false);
            }

            containerHost.updateContainerProcessing(CloudSim.clock(), getContainerAllocationPolicy().getContainerHost(containerHost).getContainerScheduler()
                    .getAllocatedMipsForContainer(containerHost));
        }

    }*/

    /**
     * Process the event for a User/Broker who wants to destroy a HOST previously created in this
     * PowerDatacenter. This PowerDatacenter may send, upon request, the status back to the
     * User/Broker.
     *
     * @param ev  a Sim_event object
     * @param ack the ack
     * @pre ev != null
     * @post $none
     */
    /*
    protected void processContainerDestroy(SimEvent ev, boolean ack) {
        Container container = (Container) ev.getData();
        getContainerAllocationPolicy().deallocateHostForContainer(container);

        if (ack) {
            int[] data = new int[3];
            data[0] = getId();
            data[1] = container.getId();
            data[2] = CloudSimTags.TRUE;

            sendNow(container.getUserId(), CloudSimTags.HOST_DESTROY_ACK, data);
        }

        getContainerHostList().remove(container);
    }
*/
    /**
     * Process the event for a User/Broker who wants to migrate a HOST. This PowerDatacenter will
     * then send the status back to the User/Broker.
     *
     * @param ev a Sim_event object
     * @pre ev != null
     * @post $none
     */
    
    /*
    protected void processHostMigrate(SimEvent ev, boolean ack) {
        Object tmp = ev.getData();
        if (!(tmp instanceof Map<?, ?>)) {
            throw new ClassCastException("The data object must be Map<String, Object>");
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> migrate = (HashMap<String, Object>) tmp;

        Container container = (Container) migrate.get("host");
        Container host = (Container) migrate.get("host");

        getContainerAllocationPolicy().deallocateHostForContainer(container);
        host.removeMigratingInContainerHost(container);
        boolean result = getContainerAllocationPolicy().allocateHostForContainer(container, host);
        if (!result) {
            Log.printLine("[Datacenter.processHostMigrate] HOST allocation to the destination host failed");
            System.exit(0);
        }

        if (ack) {
            int[] data = new int[3];
            data[0] = getId();
            data[1] = containerHost.getId();

            if (result) {
                data[2] = CloudSimTags.TRUE;
            } else {
                data[2] = CloudSimTags.FALSE;
            }
            sendNow(ev.getSource(), CloudSimTags.HOST_CREATE_ACK, data);
        }

        Log.formatLine(
                "%.2f: Migration of HOST #%d to Host #%d is completed",
                CloudSim.clock(),
                containerHost.getId(),
                host.getId());
        containerHost.setInMigration(false);
    }
    */
    /**
     * Process the event for a User/Broker who wants to migrate a HOST. This PowerDatacenter will
     * then send the status back to the User/Broker.
     *
     * @param ev a Sim_event object
     * @pre ev != null
     * @post $none
     */
    protected void processContainerMigrate(SimEvent ev, boolean ack) {

        Object tmp = ev.getData();
        if (!(tmp instanceof Map<?, ?>)) {
            throw new ClassCastException("The data object must be Map<String, Object>");
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> migrate = (HashMap<String, Object>) tmp;

        Container container = (Container) migrate.get("container");
        ContainerHost containerHost = (ContainerHost) migrate.get("host");

        getContainerAllocationPolicy().deallocateHostForContainer(container);
        if(containerHost.getContainersMigratingIn().contains(container)){
            containerHost.removeMigratingInContainer(container);}
        boolean result = getContainerAllocationPolicy().allocateHostForContainer(container, containerHost);
        if (!result) {
            Log.printLine("[Datacenter.processContainerMigrate]Container allocation to the destination host failed");
            System.exit(0);
        }
        if (containerHost.isInWaiting()){
            containerHost.setInWaiting(false);

        }

        if (ack) {
            int[] data = new int[3];
            data[0] = getId();
            data[1] = container.getId();

            if (result) {
                data[2] = CloudSimTags.TRUE;
            } else {
                data[2] = CloudSimTags.FALSE;
            }
            sendNow(ev.getSource(), containerCloudSimTags.CONTAINER_CREATE_ACK, data);
        }

         Log.formatLine(
                "%.2f: Migration of container #%d to Host #%d is completed",
                CloudSim.clock(),
                container.getId(),
                container.getHost().getId());
        container.setInMigration(false);
    }

    /**
     * Processes a Cloudlet based on the event type.
     *
     * @param ev   a Sim_event object
     * @param type event type
     * @pre ev != null
     * @pre type > 0
     * @post $none
     */
    protected void processCloudlet(SimEvent ev, int type) {
        int cloudletId = 0;
        int userId = 0;
        int hostId = 0;
        int containerId = 0;

        try { // if the sender using cloudletXXX() methods
            int data[] = (int[]) ev.getData();
            cloudletId = data[0];
            userId = data[1];
            hostId = data[2];
            containerId = data[3];
        }

        // if the sender using normal send() methods
        catch (ClassCastException c) {
            try {
                ContainerCloudlet cl = (ContainerCloudlet) ev.getData();
                cloudletId = cl.getCloudletId();
                userId = cl.getUserId();
                hostId = cl.getHostId();
                containerId = cl.getContainerId();
            } catch (Exception e) {
                Log.printConcatLine(super.getName(), ": Error in processing Cloudlet");
                Log.printLine(e.getMessage());
                return;
            }
        } catch (Exception e) {
            Log.printConcatLine(super.getName(), ": Error in processing a Cloudlet.");
            Log.printLine(e.getMessage());
            return;
        }

        // begins executing ....
        switch (type) {
            case CloudSimTags.CLOUDLET_CANCEL:
                processCloudletCancel(cloudletId, userId, hostId, containerId);
                break;

            case CloudSimTags.CLOUDLET_PAUSE:
                processCloudletPause(cloudletId, userId, hostId, containerId, false);
                break;

            case CloudSimTags.CLOUDLET_PAUSE_ACK:
                processCloudletPause(cloudletId, userId, hostId, containerId, true);
                break;

            case CloudSimTags.CLOUDLET_RESUME:
                processCloudletResume(cloudletId, userId, hostId, containerId, false);
                break;

            case CloudSimTags.CLOUDLET_RESUME_ACK:
                processCloudletResume(cloudletId, userId, hostId, containerId, true);
                break;
            default:
                break;
        }

    }

    /**
     * Process the event for a User/Broker who wants to move a Cloudlet.
     *
     * @param receivedData information about the migration
     * @param type         event tag
     * @pre receivedData != null
     * @pre type > 0
     * @post $none
     */
    protected void processCloudletMove(int[] receivedData, int type) {
        updateCloudletProcessing();

        int[] array = receivedData;
        int cloudletId = array[0];
        int userId = array[1];
        int hostId = array[2];
        int containerId = array[3];
        int hostDestId = array[4];
        int containerDestId = array[5];
        int destId = array[6];

        // get the cloudlet
        Cloudlet cl = getContainerAllocationPolicy().getContainerHost(hostId, userId).getContainer(containerId, userId)
                .getContainerCloudletScheduler().cloudletCancel(cloudletId);

        boolean failed = false;
        if (cl == null) {// cloudlet doesn't exist
            failed = true;
        } else {
            // has the cloudlet already finished?
            if (cl.getCloudletStatusString().equals("Success")) {// if yes, send it back to user
                int[] data = new int[3];
                data[0] = getId();
                data[1] = cloudletId;
                data[2] = 0;
                sendNow(cl.getUserId(), CloudSimTags.CLOUDLET_SUBMIT_ACK, data);
                sendNow(cl.getUserId(), CloudSimTags.CLOUDLET_RETURN, cl);
            }

            // prepare cloudlet for migration
            cl.setHostId(hostDestId);

            // the cloudlet will migrate from one host to another does the destination HOST exist?
            if (destId == getId()) {
                ContainerHost containerHost = getContainerAllocationPolicy().getContainerHost(hostDestId, userId);
                if (containerHost == null) {
                    failed = true;
                } else {
                    // time to transfer the files
                    double fileTransferTime = predictFileTransferTime(cl.getRequiredFiles());
                    containerHost.getContainer(containerDestId, userId).getContainerCloudletScheduler().cloudletSubmit(cl, fileTransferTime);
                }
            } else {// the cloudlet will migrate from one resource to another
                int tag = ((type == CloudSimTags.CLOUDLET_MOVE_ACK) ? CloudSimTags.CLOUDLET_SUBMIT_ACK
                        : CloudSimTags.CLOUDLET_SUBMIT);
                sendNow(destId, tag, cl);
            }
        }

        if (type == CloudSimTags.CLOUDLET_MOVE_ACK) {// send ACK if requested
            int[] data = new int[3];
            data[0] = getId();
            data[1] = cloudletId;
            if (failed) {
                data[2] = 0;
            } else {
                data[2] = 1;
            }
            sendNow(cl.getUserId(), CloudSimTags.CLOUDLET_SUBMIT_ACK, data);
        }
    }

    /**
     * Processes a Cloudlet submission.
     *
     * @param ev  a SimEvent object
     * @param ack an acknowledgement
     * @pre ev != null
     * @post $none
     */
    protected void processCloudletSubmit(SimEvent ev, boolean ack) {
        updateCloudletProcessing();

        try {            
            Job cl = (Job) ev.getData();

            // checks whether this Cloudlet has finished or not
            if (cl.isFinished()) {
                String name = CloudSim.getEntityName(cl.getUserId());
                Log.printConcatLine(getName(), ": Warning - Cloudlet #", cl.getCloudletId(), " owned by ", name,
                        " is already completed/finished.");
                /*Log.printLine("Therefore, it is not being executed again");
                Log.printLine();*/

                // NOTE: If a Cloudlet has finished, then it won't be processed.
                // So, if ack is required, this method sends back a result.
                // If ack is not required, this method don't send back a result.
                // Hence, this might cause CloudSim to be hanged since waiting
                // for this Cloudlet back.
                if (ack) {
                    int[] data = new int[3];
                    data[0] = getId();
                    data[1] = cl.getCloudletId();
                    data[2] = CloudSimTags.FALSE;

                    // unique tag = operation tag
                    int tag = CloudSimTags.CLOUDLET_SUBMIT_ACK;
                    sendNow(cl.getUserId(), tag, data);
                }

                sendNow(cl.getUserId(), CloudSimTags.CLOUDLET_RETURN, cl);

                return;
            }

            // process this Cloudlet to this CloudResource
            cl.setResourceParameter(getId(), getCharacteristics().getCostPerSecond(), getCharacteristics().getCostPerBw());

            int userId = cl.getUserId();
            int hostId = cl.getHostId();
            int containerId = cl.getContainerId();

         

            ContainerHost host = getContainerAllocationPolicy().getContainerHost(hostId, userId);            
            Container container = host.getContainer(containerId, userId);
            container.setSize(cl.getCloudletLength()); // assgin containercloudlet/job size to container Arman
            
           switch (Parameters.getCostModel()) {
                case DATACENTER:
                    // process this Cloudlet to this CloudResource
                    cl.setResourceParameter(getId(), getCharacteristics().getCostPerSecond(),
                            getCharacteristics().getCostPerBw());
                    break;
                case CONTAINER:
                   cl.setResourceParameter(getId(), container.getCost(), container.getCostPerBW());
                    break;
                default:
                    break;
                       
            }
                        
            if (cl.getClassType() == ClassType.STAGE_IN.value) {
                stageInFile2FileSystem(cl);
            }
                                    
            //double fileTransferTime = predictFileTransferTime(cl.getRequiredFiles());
            
            double fileTransferTime = 0.0;
            if (cl.getClassType() == ClassType.COMPUTE.value) {
                fileTransferTime = processDataStageInForComputeJob(cl.getFileList(), cl);
            }
             //double fileTransferTime = predictFileTransferTime(cl.getRequiredFiles());
   
            //Scheduler schedulerHost = host.getContainerScheduler();
                        
            ContainerCloudletScheduler schedulerContainer=container.getContainerCloudletScheduler();
            double estimatedFinishTime = schedulerContainer.cloudletSubmit(cl, fileTransferTime);
            updateTaskExecTime(cl, container);
            
            // if this cloudlet is in the exec queue
            if (estimatedFinishTime > 0.0 && !Double.isInfinite(estimatedFinishTime)) {
                estimatedFinishTime += fileTransferTime;
                send(getId(), estimatedFinishTime, CloudSimTags.HOST_DATACENTER_EVENT);
            }else {
                Log.printLine("Warning: You schedule cloudlet to a busy HOST");
            }
            
            if (ack) {
                int[] data = new int[3];
                data[0] = getId();
                data[1] = cl.getCloudletId();
                data[2] = CloudSimTags.TRUE;

                // unique tag = operation tag
                int tag = CloudSimTags.CLOUDLET_SUBMIT_ACK;
                sendNow(cl.getUserId(), tag, data);
            }
        } catch (ClassCastException c) {
            Log.printLine(String.format("%s.processCloudletSubmit(): ClassCastException error.", getName()));
          //by arman I commented log   c.printStackTrace();
        } catch (Exception e) {                         
              e.printStackTrace();           //by arman I commented log 
              Log.printLine(String.format("%s.processCloudletSubmit(): Exception error.", getName()));
              Log.print(e.getMessage());
        }

        checkCloudletCompletion();
    }

    /**
     * Predict file transfer time.
     *
     * @param requiredFiles the required files
     * @return the double
     */
    protected double predictFileTransferTime(List<String> requiredFiles) {
        double time = 0.0;

        for (String fileName : requiredFiles) {
            for (int i = 0; i < getStorageList().size(); i++) {
                Storage tempStorage = getStorageList().get(i);
                File tempFile = tempStorage.getFile(fileName);
                if (tempFile != null) {
                    time += tempFile.getSize() / tempStorage.getMaxTransferRate();
                    break;
                }
            }
        }
        return time;
    }

    /**
     * Processes a Cloudlet resume request.
     *
     * @param cloudletId resuming cloudlet ID
     * @param userId     ID of the cloudlet's owner
     * @param ack        $true if an ack is requested after operation
     * @param hostId       the host id
     * @pre $none
     * @post $none
     */
    protected void processCloudletResume(int cloudletId, int userId, int hostId, int containerId, boolean ack) {
        double eventTime = getContainerAllocationPolicy().getContainerHost(hostId, userId).getContainer(containerId, userId)
                .getContainerCloudletScheduler().cloudletResume(cloudletId);

        boolean status = false;
        if (eventTime > 0.0) { // if this cloudlet is in the exec queue
            status = true;
            if (eventTime > CloudSim.clock()) {
                schedule(getId(), eventTime, CloudSimTags.HOST_DATACENTER_EVENT);
            }
        }

        if (ack) {
            int[] data = new int[3];
            data[0] = getId();
            data[1] = cloudletId;
            if (status) {
                data[2] = CloudSimTags.TRUE;
            } else {
                data[2] = CloudSimTags.FALSE;
            }
            sendNow(userId, CloudSimTags.CLOUDLET_RESUME_ACK, data);
        }
    }

    /**
     * Processes a Cloudlet pause request.
     *
     * @param cloudletId resuming cloudlet ID
     * @param userId     ID of the cloudlet's owner
     * @param ack        $true if an ack is requested after operation
     * @param hostId       the host id
     * @pre $none
     * @post $none
     */
    protected void processCloudletPause(int cloudletId, int userId, int hostId, int containerId, boolean ack) {
        boolean status = getContainerAllocationPolicy().getContainerHost(hostId, userId).getContainer(containerId, userId)
                .getContainerCloudletScheduler().cloudletPause(cloudletId);

        if (ack) {
            int[] data = new int[3];
            data[0] = getId();
            data[1] = cloudletId;
            if (status) {
                data[2] = CloudSimTags.TRUE;
            } else {
                data[2] = CloudSimTags.FALSE;
            }
            sendNow(userId, CloudSimTags.CLOUDLET_PAUSE_ACK, data);
        }
    }

    /**
     * Processes a Cloudlet cancel request.
     *
     * @param cloudletId resuming cloudlet ID
     * @param userId     ID of the cloudlet's owner
     * @param hostId       the host id
     * @pre $none
     * @post $none
     */
    protected void processCloudletCancel(int cloudletId, int userId, int hostId, int containerId) {
        Cloudlet cl = getContainerAllocationPolicy().getContainerHost(hostId, userId).getContainer(containerId, userId)
                .getContainerCloudletScheduler().cloudletCancel(cloudletId);
        sendNow(userId, CloudSimTags.CLOUDLET_CANCEL, cl);
    }

    /**
     * Updates processing of each cloudlet running in this PowerDatacenter. It is necessary because
     * Hosts and VirtualMachines are simple objects, not entities. So, they don't receive events and
     * updating cloudlets inside them must be called from the outside.
     *
     * @pre $none
     * @post $none
     */
    protected void updateCloudletProcessing() {
        // if some time passed since last processing
        // R: for term is to allow loop at simulation start. Otherwise, one initial
        // simulation step is skipped and schedulers are not properly initialized
       //if (CloudSim.clock() < 0.111 || CloudSim.clock() > getLastProcessTime() + CloudSim.getMinTimeBetweenEvents()) {
                   
       if (CloudSim.clock() < 0.111 || CloudSim.clock() > getLastProcessTime() + 0.01) {
            List<? extends ContainerHost> list = getContainerAllocationPolicy().getContainerHostList();
            double smallerTime = Double.MAX_VALUE;
            // for each host...
            for (int i = 0; i < list.size(); i++) {
                ContainerHost host = list.get(i);
                // inform HOSTs to update processing
                double time = host.updateContainersProcessing(CloudSim.clock());
                // what time do we expect that the next cloudlet will finish?
                if (time < smallerTime) {
                    smallerTime = time;
                }
            }
            // gurantees a minimal interval before scheduling the event
           /* if (smallerTime < CloudSim.clock() + CloudSim.getMinTimeBetweenEvents() + 0.01) {
                smallerTime = CloudSim.clock() + CloudSim.getMinTimeBetweenEvents() + 0.01;
            }*/
            
            if (smallerTime < CloudSim.clock() + CloudSim.getMinTimeBetweenEvents() + 0.11) {
                smallerTime = CloudSim.clock() + CloudSim.getMinTimeBetweenEvents() + 0.11;
            }
            if (smallerTime != Double.MAX_VALUE) {
                schedule(getId(), (smallerTime - CloudSim.clock()), CloudSimTags.HOST_DATACENTER_EVENT);                  
            }
            setLastProcessTime(CloudSim.clock());
        }
    }

    /**
     * Verifies if some cloudlet inside this PowerDatacenter already finished. If yes, send it to
     * the User/Broker
     *
     * @pre $none
     * @post $none
     */
    protected void checkCloudletCompletion() {
        List<? extends ContainerHost> list = getContainerAllocationPolicy().getContainerHostList();
        for (int i = 0; i < list.size(); i++) {
            ContainerHost host = list.get(i);            
                for (Container container : host.getContainerList() ) {
                    while (container.getContainerCloudletScheduler().isFinishedCloudlets()) {
                        Cloudlet cl = container.getContainerCloudletScheduler().getNextFinishedCloudlet();
                        if (cl != null) {
                            sendNow(cl.getUserId(), CloudSimTags.CLOUDLET_RETURN, cl);
                            register(cl);//notice me it is important
                        }
                    }
                }            
        }
    }

    /**
     * Adds a file into the resource's storage before the experiment starts. If the file is a master
     * file, then it will be registered to the RC when the experiment begins.
     *
     * @param file a DataCloud file
     * @return a tag number denoting whether this operation is a success or not
     */
    public int addFile(File file) {
        if (file == null) {
            return DataCloudTags.FILE_ADD_ERROR_EMPTY;
        }

        if (contains(file.getName())) {
            return DataCloudTags.FILE_ADD_ERROR_EXIST_READ_ONLY;
        }

        // check storage space first
        if (getStorageList().size() <= 0) {
            return DataCloudTags.FILE_ADD_ERROR_STORAGE_FULL;
        }

        Storage tempStorage = null;
        int msg = DataCloudTags.FILE_ADD_ERROR_STORAGE_FULL;

        for (int i = 0; i < getStorageList().size(); i++) {
            tempStorage = getStorageList().get(i);
            if (tempStorage.getAvailableSpace() >= file.getSize()) {
                tempStorage.addFile(file);
                msg = DataCloudTags.FILE_ADD_SUCCESSFUL;
                break;
            }
        }

        return msg;
    }

    /**
     * Checks whether the resource has the given file.
     *
     * @param file a file to be searched
     * @return <tt>true</tt> if successful, <tt>false</tt> otherwise
     */
    protected boolean contains(File file) {
        if (file == null) {
            return false;
        }
        return contains(file.getName());
    }

    /**
     * Checks whether the resource has the given file.
     *
     * @param fileName a file name to be searched
     * @return <tt>true</tt> if successful, <tt>false</tt> otherwise
     */
    protected boolean contains(String fileName) {
        if (fileName == null || fileName.length() == 0) {
            return false;
        }

        Iterator<Storage> it = getStorageList().iterator();
        Storage storage = null;
        boolean result = false;

        while (it.hasNext()) {
            storage = it.next();
            if (storage.contains(fileName)) {
                result = true;
                break;
            }
        }

        return result;
    }

    /**
     * Deletes the file from the storage. Also, check whether it is possible to delete the file from
     * the storage.
     *
     * @param fileName the name of the file to be deleted
     * @return the error message
     */
    private int deleteFileFromStorage(String fileName) {
        Storage tempStorage = null;
        File tempFile = null;
        int msg = DataCloudTags.FILE_DELETE_ERROR;

        for (int i = 0; i < getStorageList().size(); i++) {
            tempStorage = getStorageList().get(i);
            tempFile = tempStorage.getFile(fileName);
            tempStorage.deleteFile(fileName, tempFile);
            msg = DataCloudTags.FILE_DELETE_SUCCESSFUL;
        } // end for

        return msg;
    }

    /*
     * (non-Javadoc)
     * @see cloudsim.core.SimEntity#shutdownEntity()
     */
    @Override
    public void shutdownEntity() {
        Log.printConcatLine(getName(), " is shutting down...");
    }

    /*
     * (non-Javadoc)
     * @see cloudsim.core.SimEntity#startEntity()
     */
    @Override
    public void startEntity() {
        Log.printConcatLine(getName(), " is starting...");
        // this resource should register to regional GIS.
        // However, if not specified, then register to system GIS (the
        // default CloudInformationService) entity.
        int gisID = CloudSim.getEntityId(regionalCisName);
        if (gisID == -1) {
            gisID = CloudSim.getCloudInfoServiceEntityId();
        }

        // send the registration to GIS
        sendNow(gisID, CloudSimTags.REGISTER_RESOURCE, getId());
        // Below method is for a child class to override
        registerOtherEntity();
    }

 
    /**
     * Gets the containerHost list.
     *
     * @return the containerHost list
     */
    @SuppressWarnings("unchecked")
    public <T extends ContainerHost> List<T> getContainerHostList() {
        return (List<T>) containerHostList;
    }
 
    /**
     * Gets the container list.
     *
     * @return the container list
     */
    @SuppressWarnings("unchecked")
    public <T extends Container> List<T> getContainerList() {
        return (List<T>) containerList;
    }
    /**
     * Gets the characteristics.
     *
     * @return the characteristics
     */
    protected ContainerDatacenterCharacteristics getCharacteristics() {
        return characteristics;
    }

    /**
     * Sets the characteristics.
     *
     * @param characteristics the new characteristics
     */
    protected void setCharacteristics(ContainerDatacenterCharacteristics characteristics) {
        this.characteristics = characteristics;
    }

    /**
     * Gets the regional cis name.
     *
     * @return the regional cis name
     */
    protected String getRegionalCisName() {
        return regionalCisName;
    }

    /**
     * Sets the regional cis name.
     *
     * @param regionalCisName the new regional cis name
     */
    protected void setRegionalCisName(String regionalCisName) {
        this.regionalCisName = regionalCisName;
    }

    
    /**
     * Gets the last process time.
     *
     * @return the last process time
     */
    protected double getLastProcessTime() {
        return lastProcessTime;
    }

    /**
     * Sets the last process time.
     *
     * @param lastProcessTime the new last process time
     */
    protected void setLastProcessTime(double lastProcessTime) {
        this.lastProcessTime = lastProcessTime;
    }

    /**
     * Gets the storage list.
     *
     * @return the storage list
     */
    protected List<Storage> getStorageList() {
        return storageList;
    }

    /**
     * Sets the storage list.
     *
     * @param storageList the new storage list
     */
    protected void setStorageList(List<Storage> storageList) {
        this.storageList = storageList;
    }

    
    /**
     * Sets the host list.
     *
     * @param containerHostList the new host list
     */
    protected <T extends ContainerHost> void setContainerHostList(List<T> containerHostList) {
        this.containerHostList = containerHostList;
    }

    /**
     * Gets the scheduling interval.
     *
     * @return the scheduling interval
     */
    protected double getSchedulingInterval() {
        return schedulingInterval;
    }

    /**
     * Sets the scheduling interval.
     *
     * @param schedulingInterval the new scheduling interval
     */
    protected void setSchedulingInterval(double schedulingInterval) {
        this.schedulingInterval = schedulingInterval;
    }


    public ContainerAllocationPolicy getContainerAllocationPolicy() {
        return containerAllocationPolicy;
    }

    public void setContainerAllocationPolicy(ContainerAllocationPolicy containerAllocationPolicy) {
        this.containerAllocationPolicy = containerAllocationPolicy;
    }


    public void setContainerList(List<? extends Container> containerList) {
        this.containerList = containerList;
    }


    public String getExperimentName() {
        return experimentName;
    }

    public void setExperimentName(String experimentName) {
        this.experimentName = experimentName;
    }

    public String getLogAddress() {
        return logAddress;
    }

    public void setLogAddress(String logAddress) {
        this.logAddress = logAddress;
    }
    
    
    /**
     * Update the submission time/exec time of a job
     *
     * @param job
     * @param host
     */
    private void updateTaskExecTime(Job job, Container container) {
        double start_time = job.getExecStartTime();
        for (Task task : job.getTaskList()) {
            task.setExecStartTime(start_time);
            double task_runtime = task.getCloudletLength() / container.getMips();
            start_time += task_runtime;
            //Because CloudSim would not let us update end time here
            task.setTaskFinishTime(start_time);
        }
    }

    /**
     * Stage in files for a stage-in job. For a local file system (such as
     * condor-io) add files to the local storage; For a shared file system (such
     * as NFS) add files to the shared storage
     *
     * @param cl, the job
     * @pre $none
     * @post $none
     */
    private void stageInFile2FileSystem(Job job) {
        List<FileItem> fList = job.getFileList();
        /*Log.printLine("**WFCDatacenter=>stageInFile2FileSystem**");
        Log.print(job.getDepth());*/
        for (FileItem file : fList) {
                  /*Log.printLine("          file : ");
                  Log.printLine(file);
                  Log.printLine("          getDepth : ");
                  Log.printLine(job.getDepth());*/
            switch (WFCReplicaCatalog.getFileSystem()) {
                /**
                 * For local file system, add it to local storage (data center
                 * name)
                 */
                case LOCAL:
                    
                    WFCReplicaCatalog.addFileToStorage(file.getName(), this.getName());
                    /**
                     * Is it not really needed currently but it is left for
                     * future usage
                     */
                    //ClusterStorage storage = (ClusterStorage) getStorageList().get(0);
                    //storage.addFile(file);
                    break;
                /**
                 * For shared file system, add it to the shared storage
                 */
                case SHARED:
                    WFCReplicaCatalog.addFileToStorage(file.getName(), this.getName());
                    
                    break;
                default:
                    break;
            }
        }
    }

    /*
     * Stage in for a single job (both stage-in job and compute job)
     * @param requiredFiles, all files to be stage-in
     * @param job, the job to be processed
     * @pre  $none
     * @post $none
     */
    protected double processDataStageInForComputeJob(List<FileItem> requiredFiles, Job job) throws Exception {      
         //Log.printLine("**WFCDatacenter=>processDataStageInForComputeJob**");
        
         double time = 0.0;
               for (FileItem file : requiredFiles) {
                     /*Log.printLine("          file : ");
                     Log.printLine(file);
                     Log.printLine("          getDepth : ");
                     Log.printLine(job.getDepth());*/
            //The input file is not an output File 
            if (file.isRealInputFile(requiredFiles)) {
                double maxBwth = 0.0;
                List siteList = WFCReplicaCatalog.getStorageList(file.getName());
                  /*Log.printLine("          siteList : ");
                  Log.printLine(siteList);*/
                if (siteList.isEmpty()) {
                    throw new Exception(file.getName() + " does not exist");
                }
                switch (WFCReplicaCatalog.getFileSystem()) {
                    case SHARED:
                        //stage-in job
                        /**
                         * Picks up the site that is closest
                         */
                        double maxRate = Double.MIN_VALUE;
                        for (Storage storage : getStorageList()) {
                            double rate = storage.getMaxTransferRate();
                            if (rate > maxRate) {
                                maxRate = rate;
                            }
                        }
                        //Storage storage = getStorageList().get(0);
                        time += file.getSize() / (double) Consts.MILLION / maxRate;
                        break;
                    case LOCAL:
                        int hostId = job.getHostId();
                        int userId = job.getUserId();
                        ContainerHost host = getContainerAllocationPolicy().getContainerHost(hostId, userId);
                        Container container = host.getContainer(hostId, userId);

                        boolean requiredFileStagein = true;
                        for (Iterator it = siteList.iterator(); it.hasNext();) {
                            //site is where one replica of this data is located at
                            String site = (String) it.next();
                            if (site.equals(this.getName())) {
                                continue;
                            }
                            /**
                             * This file is already in the local host and thus it
                             * is no need to transfer
                             */
                            if (site.equals(Integer.toString(hostId))) {
                                requiredFileStagein = false;
                                break;
                            }
                            double bwth;
                            if (site.equals(Parameters.SOURCE)) {
                                //transfers from the source to the HOST is limited to the HOST bw only
                                bwth = host.getBw();
                                //bwth = dcStorage.getBaseBandwidth();
                            } else {
                                //transfers between two HOSTs is limited to both HOSTs
                                bwth = Math.min(host.getBw(), getContainerAllocationPolicy().getContainerHost(Integer.parseInt(site), userId).getBw());
                                //bwth = dcStorage.getBandwidth(Integer.parseInt(site), hostId);
                            }
                            if (bwth > maxBwth) {
                                maxBwth = bwth;
                            }
                        }
                        if (requiredFileStagein && maxBwth > 0.0) {
                            time += file.getSize() / (double) Consts.MILLION / maxBwth;
                        }

                        /**
                         * For the case when storage is too small it is not
                         * handled here
                         */
                        //We should add but since CondorHost has a small capability it often fails
                        //We currently don't use this storage to do anything meaningful. It is left for future. 
                        //condorHost.addLocalFile(file);
                        WFCReplicaCatalog.addFileToStorage(file.getName(), Integer.toString(hostId));
                        break;
                }
            }
        }
        return time;
    }
    
    
    
    private void register(Cloudlet cl) {
        
        //Log.printLine("**WFCDatacenter=>register**");
        
        Task tl = (Task) cl;
        List<FileItem> fList = tl.getFileList();
        for (FileItem file : fList) {
            /*Log.printLine("          file : ");
            Log.printLine(file);
            Log.printLine("          task-getDepth : ");
            Log.printLine(tl.getDepth());*/
            if (file.getType() == Parameters.FileType.OUTPUT)//output file
            {
                switch (WFCReplicaCatalog.getFileSystem()) {
                    case SHARED:
                        WFCReplicaCatalog.addFileToStorage(file.getName(), this.getName());
                        break;
                    case LOCAL:
                        int hostId = cl.getHostId();
                        int userId = cl.getUserId();
                        ContainerHost host = getContainerAllocationPolicy().getContainerHost(hostId, userId);
                        /**
                         * Left here for future work
                         */
                        Container container = (Container) host.getContainer(hostId, userId);
                        WFCReplicaCatalog.addFileToStorage(file.getName(), Integer.toString(hostId));
                        break;
                }
            }
        }
    }
}


