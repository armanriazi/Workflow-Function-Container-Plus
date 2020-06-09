package org.cloudbus.cloudsim.container.core;

import org.cloudbus.cloudsim.container.containerVmProvisioners.ContainerVmBwProvisioner;
import org.cloudbus.cloudsim.container.containerVmProvisioners.ContainerVmPe;
import org.cloudbus.cloudsim.container.lists.ContainerVmPeList;
import org.cloudbus.cloudsim.container.containerVmProvisioners.ContainerVmRamProvisioner;
import org.cloudbus.cloudsim.container.schedulers.ContainerVmScheduler;
import org.cloudbus.cloudsim.HostStateHistoryEntry;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerBwProvisioner;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerPe;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerRamProvisioner;
import org.cloudbus.cloudsim.container.schedulers.ContainerScheduler;

/**
 * Created by sareh on 14/07/15.
 */
public class ContainerHostDynamicWorkload extends ContainerHost{


        /** The utilization mips. */
        private double utilizationMips;

        /** The previous utilization mips. */
        private double previousUtilizationMips;

        /** The state history. */
        private final List<HostStateHistoryEntry> stateHistory = new LinkedList<HostStateHistoryEntry>();

        /**
         * Instantiates a new host.
         *
         * @param id the id
         * @param ramProvisioner the ram provisioner
         * @param bwProvisioner the bw provisioner
         * @param storage the storage
         * @param peList the pe list
         * @param containerScheduler the VM scheduler
         */
        public ContainerHostDynamicWorkload(
                int id,
                ContainerRamProvisioner ramProvisioner,
                ContainerBwProvisioner bwProvisioner,
                long storage,
                List<? extends ContainerPe> peList,
                ContainerScheduler containerScheduler) {
            super(id, ramProvisioner, bwProvisioner, storage, peList, containerScheduler);
            setUtilizationMips(0);
            setPreviousUtilizationMips(0);
        }

        /*
         * (non-Javadoc)
         * @see cloudsim.Host#updateVmsProcessing(double)
         */
        @Override
        public double updateContainersProcessing(double currentTime) {
            double smallerTime = super.updateContainersProcessing(currentTime);
            setPreviousUtilizationMips(getUtilizationMips());
            setUtilizationMips(0);
            double hostTotalRequestedMips = 0;

            for (Container container : getContainerList()) {
                getContainerScheduler().deallocatePesForAllContainer(container);
            }

            for (Container  container : getContainerList()) {
                getContainerScheduler().allocatePesForContainer(container, container.getCurrentRequestedMips());
            }

            for (Container  container : getContainerList()) {
                double totalRequestedMips = container.getCurrentRequestedTotalMips();
                double totalAllocatedMips = getContainerScheduler().getTotalAllocatedMipsForContainer(container);

                if (!Log.isDisabled()) {
                /*by arman I commented log  Log.formatLine(
                            "%.2f: [Host #" + getId() + "] Total allocated MIPS for VM #" + containerVm.getId()
                                    + " (Host #" + containerVm.getHost().getId()
                                    + ") is %.2f, was requested %.2f out of total %.2f (%.2f%%)",
                            CloudSim.clock(),
                            totalAllocatedMips,
                            totalRequestedMips,
                            containerVm.getMips(),
                            totalRequestedMips / containerVm.getMips() * 100);
                        */
                    List<ContainerPe> pes = getContainerScheduler().getPesAllocatedForCONTAINER(container);
                    StringBuilder pesString = new StringBuilder();
                    for (ContainerPe pe : pes) {
                        pesString.append(String.format(" PE #" + pe.getId() + ": %.2f.", pe.getContainerPeProvisioner()
                                .getTotalAllocatedMipsForContainer(container)));
                    }
                    /*by arman I commented logLog.formatLine(
                            "%.2f: [Host #" + getId() + "] MIPS for VM #" + containerVm.getId() + " by PEs ("
                                    + getNumberOfPes() + " * " + getContainerVmScheduler().getPeCapacity() + ")."
                                    + pesString,
                            CloudSim.clock());
                    */
                }

                if (getContainersMigratingIn().contains(container)) {
                 /*by arman I commented log   Log.formatLine("%.2f: [Host #" + getId() + "] VM #" + containerVm.getId()
                            + " is being migrated to Host #" + getId(), CloudSim.clock());
                    */
                } else {
                    if (totalAllocatedMips + 0.1 < totalRequestedMips) {
                        /*by arman I commented logLog.formatLine("%.2f: [Host #" + getId() + "] Under allocated MIPS for VM #" + containerVm.getId()
                                + ": %.2f", CloudSim.clock(), totalRequestedMips - totalAllocatedMips);
                        */
                    }

                    containerVm.addStateHistoryEntry(
                            currentTime,
                            totalAllocatedMips,
                            totalRequestedMips,
                            (containerVm.isInMigration() && !getContainersMigratingIn().contains(containerVm)));

                    if (containerVm.isInMigration()) {
                       /*by arman I commented log Log.formatLine(
                                "%.2f: [Host #" + getId() + "] VM #" + containerVm.getId() + " is in migration",
                                CloudSim.clock());
                        */
                        totalAllocatedMips /= 0.9; // performance degradation due to migration - 10%
                    }
                }

                setUtilizationMips(getUtilizationMips() + totalAllocatedMips);
                hostTotalRequestedMips += totalRequestedMips;
            }

            addStateHistoryEntry(
                    currentTime,
                    getUtilizationMips(),
                    hostTotalRequestedMips,
                    (getUtilizationMips() > 0));

            return smallerTime;
        }

        /**
         * Gets the completed containers.
         *
         * @return the completed containers
         */
        public List<ContainerVm> getCompletedVms() {
            List<ContainerVm> containersToRemove = new ArrayList<>();
            for (ContainerVm containerVm : getVmList()) {
                if (containerVm.isInMigration()) {
                    continue;
                }
//                if the  container is in waiting state then dont kill it just waite !!!!!!!!!
                 if(containerVm.isInWaiting()){
                     continue;
                 }
//              if (containerVm.getCurrentRequestedTotalMips() == 0) {
//                    containersToRemove.add(containerVm);
//                }
                if(containerVm.getNumberOfContainers()==0 ){
                    containersToRemove.add(containerVm);
                }
            }
            return containersToRemove;
        }



    /**
     * Gets the completed containers.
     *
     * @return the completed containers
     */
    public int getNumberofContainers() {
        int numberofContainers = 0;
        for (ContainerVm containerVm : getVmList()) {
            numberofContainers += containerVm.getNumberOfContainers();
            Log.print("The number of containers in VM# " + containerVm.getId()+"is: "+ containerVm.getNumberOfContainers());
            Log.printLine();
        }
        return numberofContainers;
    }




        /**
         * Gets the max utilization among by all PEs.
         *
         * @return the utilization
         */
        public double getMaxUtilization() {
            return ContainerVmPeList.getMaxUtilization(getPeList());
        }

        /**
         * Gets the max utilization among by all PEs allocated to the VM.
         *
         * @param container the container
         * @return the utilization
         */
        public double getMaxUtilizationAmongVmsPes(ContainerVm container) {
            return ContainerVmPeList.getMaxUtilizationAmongVmsPes(getPeList(), container);
        }

        /**
         * Gets the utilization of memory.
         *
         * @return the utilization of memory
         */
        public double getUtilizationOfRam() {
            return getContainerVmRamProvisioner().getUsedVmRam();
        }

        /**
         * Gets the utilization of bw.
         *
         * @return the utilization of bw
         */
        public double getUtilizationOfBw() {
            return getContainerVmBwProvisioner().getUsedBw();
        }

        /**
         * Get current utilization of CPU in percentage.
         *
         * @return current utilization of CPU in percents
         */
        public double getUtilizationOfCpu() {
            double utilization = getUtilizationMips() / getTotalMips();
            if (utilization > 1 && utilization < 1.01) {
                utilization = 1;
            }
            return utilization;
        }

        /**
         * Gets the previous utilization of CPU in percentage.
         *
         * @return the previous utilization of cpu
         */
        public double getPreviousUtilizationOfCpu() {
            double utilization = getPreviousUtilizationMips() / getTotalMips();
            if (utilization > 1 && utilization < 1.01) {
                utilization = 1;
            }
            return utilization;
        }

        /**
         * Get current utilization of CPU in MIPS.
         *
         * @return current utilization of CPU in MIPS
         */
        public double getUtilizationOfCpuMips() {
            return getUtilizationMips();
        }

        /**
         * Gets the utilization mips.
         *
         * @return the utilization mips
         */
        public double getUtilizationMips() {
            return utilizationMips;
        }

        /**
         * Sets the utilization mips.
         *
         * @param utilizationMips the new utilization mips
         */
        protected void setUtilizationMips(double utilizationMips) {
            this.utilizationMips = utilizationMips;
        }

        /**
         * Gets the previous utilization mips.
         *
         * @return the previous utilization mips
         */
        public double getPreviousUtilizationMips() {
            return previousUtilizationMips;
        }

        /**
         * Sets the previous utilization mips.
         *
         * @param previousUtilizationMips the new previous utilization mips
         */
        protected void setPreviousUtilizationMips(double previousUtilizationMips) {
            this.previousUtilizationMips = previousUtilizationMips;
        }

        /**
         * Gets the state history.
         *
         * @return the state history
         */
        public List<HostStateHistoryEntry> getStateHistory() {
            return stateHistory;
        }

        /**
         * Adds the state history entry.
         *
         * @param time the time
         * @param allocatedMips the allocated mips
         * @param requestedMips the requested mips
         * @param isActive the is active
         */
        public
        void
        addStateHistoryEntry(double time, double allocatedMips, double requestedMips, boolean isActive) {

            HostStateHistoryEntry newState = new HostStateHistoryEntry(
                    time,
                    allocatedMips,
                    requestedMips,
                    isActive);
            if (!getStateHistory().isEmpty()) {
                HostStateHistoryEntry previousState = getStateHistory().get(getStateHistory().size() - 1);
                if (previousState.getTime() == time) {
                    getStateHistory().set(getStateHistory().size() - 1, newState);
                    return;
                }
            }
            getStateHistory().add(newState);
        }

    }

