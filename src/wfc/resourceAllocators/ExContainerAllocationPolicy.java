package wfc.resourceAllocators;

import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.core.ContainerHost;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.cloudbus.cloudsim.container.core.ContainerHost;
import org.cloudbus.cloudsim.container.resourceAllocators.ContainerAllocationPolicy;

/**
 * Created by Arman
 */
public  class ExContainerAllocationPolicy extends ContainerAllocationPolicy{

        /** The container table. */
        private final Map<String, ContainerHost> containerTable = new HashMap<>();

        /**
         * Instantiates a new power vm allocation policy abstract.
         *
         */
        public ExContainerAllocationPolicy() {
            super();
        }

        /*
         * (non-Javadoc)
         * @see org.cloudbus.cloudsim.HostAllocationPolicy#allocateHostForHost(org.cloudbus.cloudsim.Host)
         */
        @Override
        public boolean allocateHostForContainer(Container container, List<ContainerHost> containerHostList) {
            setContainerHostList(containerHostList);
            return allocateHostForContainer(container, findHostForContainer(container));
        }

        /*
         * (non-Javadoc)
         * @see org.cloudbus.cloudsim.HostAllocationPolicy#allocateHostForHost(org.cloudbus.cloudsim.Host,
         * org.cloudbus.cloudsim.Host)
         */
        @Override
        public boolean allocateHostForContainer(Container container, ContainerHost containerHost) {
            if (containerHost == null) {
                Log.formatLine("%.2f: No suitable HOST found for Container#" + container.getId() + "\n", CloudSim.clock());
                return false;
            }
            if (containerHost.containerCreate(container)) { // if vm has been succesfully created in the host
                getContainerTable().put(container.getUid(), containerHost);
//                container.setHost(containerHost);
                Log.formatLine(
                        "%.2f: Container #" + container.getId() + " has been allocated to the HOST #" + containerHost.getId(),
                        CloudSim.clock());
                return true;
            }
            Log.formatLine(
                    "%.2f: Creation of Container #" + container.getId() + " on the Host #" + containerHost.getId() + " failed\n",
                    CloudSim.clock());
            return false;
        }

        /**
         * Find host for vm.
         *
         * @param container the vm
         * @return the power host
         */
        public ContainerHost findHostForContainer(Container container) {
            for (ContainerHost containerHost : getContainerHostList()) {
//                Log.printConcatLine("Trying vm #",containerHost.getId(),"For container #", container.getId());
                if (containerHost.isSuitableForContainer(container)) {
                    return containerHost;
                }
            }
            return null;
        }

        
        /*
         * (non-Javadoc)
         * @see org.cloudbus.cloudsim.HostAllocationPolicy#deallocateHostForHost(org.cloudbus.cloudsim.Host)
         */
        @Override
        public void deallocateHostForContainer(Container container) {
            ContainerHost containerHost = getContainerTable().remove(container.getUid());
            if (containerHost != null) {
                containerHost.containerDestroy(container);
            }
        }

        /*
         * (non-Javadoc)
         * @see org.cloudbus.cloudsim.HostAllocationPolicy#getHost(org.cloudbus.cloudsim.Host)
         */
        @Override
        public ContainerHost getContainerHost(Container container) {
            return getContainerTable().get(container.getUid());
        }

        /*
         * (non-Javadoc)
         * @see org.cloudbus.cloudsim.HostAllocationPolicy#getHost(int, int)
         */
        @Override
        public ContainerHost getContainerHost(int containerId, int userId) {
            return getContainerTable().get(Container.getUid(userId, containerId));
        }

        /**
         * Gets the vm table.
         *
         * @return the vm table
         */
        public Map<String, ContainerHost> getContainerTable() {
            return containerTable;
        }

        
        @Override
        public List<Map<String, Object>> optimizeAllocation(List<? extends Container> containerList) {
            return null;
        }
    }



