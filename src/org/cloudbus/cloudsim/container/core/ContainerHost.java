package org.cloudbus.cloudsim.container.core;

import wfc.core.WFCDatacenter;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerBwProvisioner;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerRamProvisioner;
import org.cloudbus.cloudsim.container.schedulers.ContainerScheduler;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerPe;
import org.cloudbus.cloudsim.container.lists.ContainerPeList;
import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import java.util.ArrayList;
import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.summarizingDouble;
import java.util.stream.Stream;

/**
 * Created by sareh on 10/07/15.
 */
public class ContainerHost {

        // Added By Arman From CondorVm
    private int state;       


    /**
     * The id.
     */
    private int id;

    /**
     * The storage.
     */
    private long storage;

    /**
     * The ram provisioner.
     */
    private ContainerRamProvisioner containerRamProvisioner;

    /**
     * The bw provisioner.
     */
    private ContainerBwProvisioner containerBwProvisioner;

    /**
     * The allocation policy.
     */
    private ContainerScheduler containerScheduler;

    /**
     * The container list.
     */
    private final List<? extends Container> containerList = new ArrayList<>();
    /**
     * The container list.
     */

    /**
     * The pe list.
     */
    private List<? extends ContainerPe> peList;

        
    /**
     * Tells whether this machine is working properly or has failed.
     */
    private boolean failed;

     /**
     * In waiting flag. shows that container is waiting for containers to come.
     */
    private boolean inWaiting;
     /**
     * Gets the containers migrating in.
     *
     * @return the containers migrating in
     */
    public List<Container> getContainersMigratingIn() {
        return containersMigratingIn;
    }

    public ContainerRamProvisioner getContainerRamProvisioner() {
        return containerRamProvisioner;
    }

    public void setContainerRamProvisioner(ContainerRamProvisioner containerRamProvisioner) {
        this.containerRamProvisioner = containerRamProvisioner;
    }


    public ContainerBwProvisioner getContainerBwProvisioner() {
        return containerBwProvisioner;
    }

    public void setContainerBwProvisioner(ContainerBwProvisioner containerBwProvisioner) {
        this.containerBwProvisioner = containerBwProvisioner;
    }
    /**
     * The containerms migrating in.
     */
    private final List<Container> containersMigratingIn = new ArrayList<>();
    /**
     * The datacenter where the host is placed.
     */
    private WFCDatacenter datacenter;

    /**
     * Instantiates a new host.
     *
     * @param id             the id
     * @param containerRamProvisioner the ram provisioner
     * @param containerBwProvisioner  the bw provisioner
     * @param storage        the storage
     * @param peList         the pe list
     * @param containerScheduler    the container scheduler
     */
    public ContainerHost(
            int id,
            ContainerRamProvisioner containerRamProvisioner,
            ContainerBwProvisioner containerBwProvisioner,
            long storage,
            List<? extends ContainerPe> peList,
            ContainerScheduler containerScheduler) {
        setId(id);
        setContainerRamProvisioner(containerRamProvisioner);
        setContainerBwProvisioner(containerBwProvisioner);
        setStorage(storage);
        setContainerScheduler(containerScheduler);
        setPeList(peList);
        setFailed(false);

    }

    /**
     * Requests updating of processing of cloudlets in the containers running in this host.
     *
     * @param currentTime the current time
     * @return expected time of completion of the next cloudlet in all containers in this host.
     * Double.MAX_VALUE if there is no future events expected in this host
     * @pre currentTime >= 0.0
     * @post $none
     */
    public double updateContainersProcessing(double currentTime) {
        double smallerTime = Double.MAX_VALUE;

        for (Container container : getList()) {
            double time = container.updateContainerProcessing(currentTime, getContainerScheduler().getAllocatedMipsForContainer(container));
            if (time > 0.0 && time < smallerTime) {
                smallerTime = time;
            }
        }

        return smallerTime;
    }

    /**
     * Adds the migrating in container.
     *
     * @param container the container
     */
    public void addMigratingInContainer(Container container) {
        //Log.printLine("Host: addMigratingInContainer:......");
        container.setInMigration(true);

        if (!getsMigratingIn().contains(container)) {
            if (getStorage() < container.getSize()) {
                Log.printConcatLine("[Scheduler.addMigratingInContainer] Allocation of CONTAINER #", container.getId(), " to Host #",
                        getId(), " failed by storage");
                System.exit(0);
            }

            if (!getContainerRamProvisioner().allocateRamForContainer(container, container.getCurrentRequestedRam())) {
                Log.printConcatLine("[Scheduler.addMigratingInContainer] Allocation of CONTAINER #", container.getId(), " to Host #",
                        getId(), " failed by RAM");
                System.exit(0);
            }

            if (!getContainerBwProvisioner().allocateBwForContainer(container, container.getCurrentRequestedBw())) {
                Log.printConcatLine("[Scheduler.addMigratingInContainer] Allocation of CONTAINER #", container.getId(), " to Host #",
                        getId(), " failed by BW");
                System.exit(0);
            }

            getContainerScheduler().getContainersMigratingIn().add(container.getUid());
            if (!getContainerScheduler().allocatePesForContainer(container, container.getCurrentRequestedMips())) {
                Log.printConcatLine("[Scheduler.addMigratingInContainer] Allocation of CONTAINER #", container.getId(), " to Host #",
                        getId(), " failed by MIPS");
                System.exit(0);
            }

            setStorage(getStorage() - container.getSize());

            getsMigratingIn().add(container);
            getList().add(container);
            updateContainersProcessing(CloudSim.clock());
            container.getHost().updateContainersProcessing(CloudSim.clock());
        }
    }

        /**
         * Removes the migrating in vm.
         *
         * @param vm the vm
         */
        public void removeMigratingInContainer(Container vm) {
            containerDeallocate(vm);
            getsMigratingIn().remove(vm);
            getList().remove(vm);
            getContainerScheduler().getContainersMigratingIn().remove(vm.getUid());
            vm.setInMigration(false);
        }

    /**
     * Reallocate migrating in vms.
     */
    public void reallocateMigratingInContainers() {
        for (Container container : getsMigratingIn()) {
            if (!getList().contains(container)) {
                getList().add(container);
            }
            if (!getContainerScheduler().getContainersMigratingIn().contains(container.getUid())) {
                getContainerScheduler().getContainersMigratingIn().add(container.getUid());
            }
            getContainerRamProvisioner().allocateRamForContainer(container, container.getCurrentRequestedRam());
            getContainerBwProvisioner().allocateBwForContainer(container, container.getCurrentRequestedBw());
            getContainerScheduler().allocatePesForContainer(container, container.getCurrentRequestedMips());
            setStorage(getStorage() - container.getSize());
        }
    }

    /**
     * Checks if is suitable for vm.
     *
     * @param vm the vm
     * @return true, if is suitable for vm
     */
    public boolean isSuitableForContainer(Container vm) {
        //Log.printLine("Host: Is suitable for CONTAINER???......");
        return (getContainerScheduler().getPeCapacity() >= vm.getCurrentRequestedMaxMips()
                && getContainerScheduler().getAvailableMips() >= vm.getCurrentRequestedTotalMips()
                && getContainerRamProvisioner().isSuitableForContainer(vm, vm.getCurrentRequestedRam()) && getContainerBwProvisioner()
                .isSuitableForContainer(vm, vm.getCurrentRequestedBw()));
    }

    /**
     * Allocates PEs and memory to a new CONTAINER in the Host.
     *
     * @param vm  being started
     * @return $true if the CONTAINER could be started in the host; $false otherwise
     * @pre $none
     * @post $none
     */
    public boolean containerCreate(Container container) {
        //Log.printLine("Host: Create CONTAINER???......" + vm.getId());
        if (getStorage() < container.getSize()) {
            Log.printConcatLine("[Scheduler.containerCreate] Allocation of container #", container.getId(), " to Host #", getId(),
                    " failed by storage");
            return false;
        }

        if (!getContainerRamProvisioner().allocateRamForContainer(container, container.getCurrentRequestedRam())) {
            Log.printConcatLine("[Scheduler.containerCreate] Allocation of container #", container.getId(), " to Host #", getId(),
                    " failed by RAM");
            return false;
        }

        if (!getContainerBwProvisioner().allocateBwForContainer(container, container.getCurrentRequestedBw())) {
            Log.printConcatLine("[Scheduler.containerCreate] Allocation of container #", container.getId(), " to Host #", getId(),
                    " failed by BW");
            getContainerRamProvisioner().deallocateRamForContainer(container);
            return false;
        }

        if (!getContainerScheduler().allocatePesForContainer(container, container.getCurrentRequestedMips())) {
            Log.printConcatLine("[Scheduler.containerCreate] Allocation of container #", container.getId(), " to Host #", getId(),
                    " failed by MIPS");
            getContainerRamProvisioner().deallocateRamForContainer(container);
            getContainerBwProvisioner().deallocateBwForContainer(container);
            return false;
        }

        setStorage(getStorage() - container.getSize());
        getList().add(container);
        container.setHost(this);
        return true;
    }

    /**
     * Destroys a CONTAINER running in the host.
     *
     * @param container the CONTAINER
     * @pre $none
     * @post $none
     */
    public void containerDestroy(Container container) {
        //Log.printLine("Host:  Destroy :.... " + container.getId());
        if (container != null) {
            containerDeallocate(container);
            getList().remove(container);
            container.setHost(null);
        }
    }

    /**
     * Destroys all CONTAINERs running in the host.
     *
     * @pre $none
     * @post $none
     */
    public void containerDestroyAll() {
        //Log.printLine("Host: Destroy all s");
        containerDeallocateAll();
        for (Container container : getList()) {
            container.setHost(null);
            setStorage(getStorage() + container.getSize());
        }
        getList().clear();
    }

    /**
     * Deallocate all hostList for the CONTAINER.
     *
     * @param container the CONTAINER
     */
    protected void containerDeallocate(Container container) {
        //Log.printLine("Host: Deallocated the CONTAINER:......" + container.getId());
        getContainerRamProvisioner().deallocateRamForContainer(container);
        getContainerBwProvisioner().deallocateBwForContainer(container);
        getContainerScheduler().deallocatePesForContainer(container);
        setStorage(getStorage() + container.getSize());
    }

    /**
     * Deallocate all hostList for the CONTAINER.
     */
    protected void containerDeallocateAll() {
        //Log.printLine("Host: Deallocate all the s......");
        getContainerRamProvisioner().deallocateRamForAllContainers();
        getContainerBwProvisioner().deallocateBwForAllContainers();
        getContainerScheduler().deallocatePesForAllContainers();
    }

    /**
     * Returns a CONTAINER object.
     *
     * @param vmId   the vm id
     * @param userId ID of CONTAINER's owner
     * @return the virtual machine object, $null if not found
     * @pre $none
     * @post $none
     */
    public Container getContainer(int vmId, int userId) {
        //Log.printLine("Host: get the vm......" + vmId);
        //Log.printLine("Host: the vm list size:......" + getList().size());
        for (Container container : getList()) {
            if (container.getId() == vmId && container.getUserId() == userId) {
                return container;
            }
        }
        return null;
    }

    /**
     * Gets the pes number.
     *
     * @return the pes number
     */
    public int getNumberOfPes() {
        //Log.printLine("Host: get the peList Size......" + getPeList().size());
        return getPeList().size();
    }

    /**
     * Gets the free pes number.
     *
     * @return the free pes number
     */
    public int getNumberOfFreePes() {
        //Log.printLine("Host: get the free Pes......" + ContainerPeList.getNumberOfFreePes(getPeList()));
        return ContainerPeList.getNumberOfFreePes(getPeList());
    }

    /**
     * Gets the total mips.
     *
     * @return the total mips
     */
    public int getTotalMips() {
        //Log.printLine("Host: get the total mips......" + ContainerPeList.getTotalMips(getPeList()));
        return ContainerPeList.getTotalMips(getPeList());
    }

    /**
     * Allocates PEs for a CONTAINER.
     *
     * @param container        the vm
     * @param mipsShare the mips share
     * @return $true if this policy allows a new CONTAINER in the host, $false otherwise
     * @pre $none
     * @post $none
     */
    public boolean allocatePesForContainer(Container container, List<Double> mipsShare) {
        //Log.printLine("Host: allocate Pes for :......" + container.getId());
        return getContainerScheduler().allocatePesForContainer(container, mipsShare);
    }

    /**
     * Releases PEs allocated to a CONTAINER.
     *
     * @param container the vm
     * @pre $none
     * @post $none
     */
    public void deallocatePesForContainer(Container container) {
        //Log.printLine("Host: deallocate Pes for :......" + container.getId());
        getContainerScheduler().deallocatePesForContainer(container);
    }

    /**
     * Returns the MIPS share of each Pe that is allocated to a given CONTAINER.
     *
     * @param container the vm
     * @return an array containing the amount of MIPS of each pe that is available to the CONTAINER
     * @pre $none
     * @post $none
     */
    public List<Double> getAllocatedMipsForContainer(Container container) {
        //Log.printLine("Host: get allocated Pes for :......" + container.getId());
        return getContainerScheduler().getAllocatedMipsForContainer(container);
    }

    /**
     * Gets the total allocated MIPS for a CONTAINER over all the PEs.
     *
     * @param container the vm
     * @return the allocated mips for vm
     */
    public double getTotalAllocatedMipsForContainer(Container container) {
        //Log.printLine("Host: total allocated Pes for :......" + container.getId());
        return getContainerScheduler().getTotalAllocatedMipsForContainer(container);
    }

    /**
     * Returns maximum available MIPS among all the PEs.
     *
     * @return max mips
     */
    public double getMaxAvailableMips() {
        //Log.printLine("Host: Maximum Available Pes:......");
        return getContainerScheduler().getMaxAvailableMips();
    }

    /**
     * Gets the free mips.
     *
     * @return the free mips
     */
    public double getAvailableMips() {
        //Log.printLine("Host: Get available Mips");
        return getContainerScheduler().getAvailableMips();
    }

    /**
     * Gets the machine bw.
     *
     * @return the machine bw
     * @pre $none
     * @post $result > 0
     */
    public long getBw() {
        //Log.printLine("Host: Get BW:......" + getContainerBwProvisioner().getBw());
        return getContainerBwProvisioner().getBw();
    }

    /**
     * Gets the machine memory.
     *
     * @return the machine memory
     * @pre $none
     * @post $result > 0
     */
    public float getRam() {
        //Log.printLine("Host: Get Ram:......" + getContainerRamProvisioner().getRam());

        return getContainerRamProvisioner().getRam();
    }

    /**
     * Gets the machine storage.
     *
     * @return the machine storage
     * @pre $none
     * @post $result >= 0
     */
    public long getStorage() {
        return storage;
    }

    /**
     * Gets the id.
     *
     * @return the id
     */
    public int getId() {
        return id;
    }

    /**
     * Sets the id.
     *
     * @param id the new id
     */
    protected void setId(int id) {
        this.id = id;
    }


    /**
     * Gets the CONTAINER scheduler.
     *
     * @return the CONTAINER scheduler
     */
    public ContainerScheduler getContainerScheduler() {
        return containerScheduler;
    }

    /**
     * Sets the CONTAINER scheduler.
     *
     * @param vmScheduler the vm scheduler
     */
    protected void setContainerScheduler(ContainerScheduler containerScheduler) {
        this.containerScheduler = containerScheduler;
    }

      public boolean isInWaiting() {
        return inWaiting;
    }

    public void setInWaiting(boolean inWaiting) {
        this.inWaiting = inWaiting;
    }
 
 
    /**
     * Gets the pe list.
     *
     * @param <T> the generic type
     * @return the pe list
     */
    @SuppressWarnings("unchecked")
    public <T extends ContainerPe> List<T> getPeList() {
        return (List<T>) peList;
    }

    /**
     * Sets the pe list.
     *
     * @param <T>    the generic type
     * @param containerPeList the new pe list
     */
    protected <T extends ContainerPe> void setPeList(List<T> containerPeList) {
        this.peList = containerPeList;
    }

    /**
     * Gets the container list.
     *
     * @param <T> the generic type
     * @return the container list
     */
    @SuppressWarnings("unchecked")
    public <T extends Container> List<T> getList() {
        return (List<T>) containerList;
    }

    /**
     * Sets the storage.
     *
     * @param storage the new storage
     */
    protected void setStorage(long storage) {
        this.storage = storage;
    }

    /**
     * Checks if is failed.
     *
     * @return true, if is failed
     */
    public boolean isFailed() {
        return failed;
    }

    /**
     * Sets the PEs of this machine to a FAILED status. NOTE: <tt>resName</tt> is used for debugging
     * purposes, which is <b>ON</b> by default. Use {@link #setFailed(boolean)} if you do not want
     * this information.
     *
     * @param resName the name of the resource
     * @param failed  the failed
     * @return <tt>true</tt> if successful, <tt>false</tt> otherwise
     */
    public boolean setFailed(String resName, boolean failed) {
        // all the PEs are failed (or recovered, depending on fail)
        this.failed = failed;
        ContainerPeList.setStatusFailed(getPeList(), resName, getId(), failed);
        return true;
    }

    /**
     * Sets the PEs of this machine to a FAILED status.
     *
     * @param failed the failed
     * @return <tt>true</tt> if successful, <tt>false</tt> otherwise
     */
    public boolean setFailed(boolean failed) {
        // all the PEs are failed (or recovered, depending on fail)
        this.failed = failed;
        ContainerPeList.setStatusFailed(getPeList(), failed);
        return true;
    }

    /**
     * Sets the particular Pe status on this Machine.
     *
     * @param peId   the pe id
     * @param status Pe status, either <tt>Pe.FREE</tt> or <tt>Pe.BUSY</tt>
     * @return <tt>true</tt> if the Pe status has changed, <tt>false</tt> otherwise (Pe id might not
     * be exist)
     * @pre peID >= 0
     * @post $none
     */
    public boolean setPeStatus(int peId, int status) {
        return ContainerPeList.setPeStatus(getPeList(), peId, status);
    }

    
    /**
     * Gets the containers migrating in.
     *
     * @return the containers migrating in
     */
    public List<Container> getsMigratingIn() {
        return containersMigratingIn;
    }

    /**
     * Gets the data center.
     *
     * @return the data center where the host runs
     */
    public WFCDatacenter getDatacenter() {
        return datacenter;
    }

    /**
     * Sets the data center.
     *
     * @param datacenter the data center from this host
     */
    public void setDatacenter(WFCDatacenter datacenter) {
        this.datacenter = datacenter;
    }

      /**
     * Gets the container list.
     *
     * @param <T> the generic type
     * @return the container list
     */
    @SuppressWarnings("unchecked")
    public <T extends Container> List<T> getContainerList() {
        return (List<T>) containerList;
    }

    
    public int getNumberOfContainers() {
        int c =0;
        for(Container container:getContainerList()){
            if(!getContainersMigratingIn().contains(container)){
                c++;
            }
        }
        return c;
    }
    
   

}


