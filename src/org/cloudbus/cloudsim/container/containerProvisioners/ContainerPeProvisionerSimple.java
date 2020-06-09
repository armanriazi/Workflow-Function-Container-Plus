package org.cloudbus.cloudsim.container.containerProvisioners;


import org.cloudbus.cloudsim.container.core.Container;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Arman on 2020.
 */
public class ContainerPeProvisionerSimple extends ContainerPeProvisioner {


    /** The pe table. */
    private Map<String, List<Double>> peTable;

    /**
     * Creates the PeProvisionerSimple object.
     *
     * @param availableMips the available mips
     *
     * @pre $none
     * @post $none
     */
    public ContainerPeProvisionerSimple(double availableMips) {
        super(availableMips);
        setPeTable(new HashMap<String, ArrayList<Double>>());
    }




    @Override
    public boolean allocateMipsForContainer(Container container, double mips) {

        return allocateMipsForContainer(container.getUid(), mips);
    }

    @Override
    public boolean allocateMipsForContainer(String containerUid, double mips) {
        if (getAvailableMips() < mips) {
            return false;
        }

        List<Double> allocatedMips;

        if (getPeTable().containsKey(containerUid)) {
            allocatedMips = getPeTable().get(containerUid);
        } else {
            allocatedMips = new ArrayList<>();
        }

        allocatedMips.add(mips);

        setAvailableMips(getAvailableMips() - mips);
        getPeTable().put(containerUid, allocatedMips);

        return true;
    }

    @Override
    public boolean allocateMipsForContainer(Container container, List<Double> mips) {
        int totalMipsToAllocate = 0;
        for (double _mips : mips) {
            totalMipsToAllocate += _mips;
        }

        if (getAvailableMips() + getTotalAllocatedMipsForContainer(container)< totalMipsToAllocate) {
            return false;
        }

        setAvailableMips(getAvailableMips() + getTotalAllocatedMipsForContainer(container)- totalMipsToAllocate);

        getPeTable().put(container.getUid(), mips);

        return true;
    }

    @Override
    public List<Double> getAllocatedMipsForContainer(Container container) {
        if (getPeTable().containsKey(container.getUid())) {
            return getPeTable().get(container.getUid());
        }
        return null;
    }

    @Override
    public double getTotalAllocatedMipsForContainer(Container container) {
        if (getPeTable().containsKey( container.getUid())) {
            double totalAllocatedMips = 0.0;
            for (double mips : getPeTable().get(container.getUid())) {
                totalAllocatedMips += mips;
            }
            return totalAllocatedMips;
        }
        return 0;
    }

    @Override
    public double getAllocatedMipsForContainerByVirtualPeId(Container container, int peId) {
        if (getPeTable().containsKey(container.getUid())) {
            try {
                return getPeTable().get(container.getUid()).get(peId);
            } catch (Exception e) {
            }
        }
        return 0;
    }

    @Override
    public void deallocateMipsForContainer(Container container) {
        if (getPeTable().containsKey(container.getUid())) {
            for (double mips : getPeTable().get(container.getUid())) {
                setAvailableMips(getAvailableMips() + mips);
            }
            getPeTable().remove(container.getUid());
        }
    }

    @Override
    public void deallocateMipsForAllContainers() {
        super.deallocateMipsForAllContainers();
        getPeTable().clear();
    }
    /**
     * Gets the pe table.
     *
     * @return the peTable
     */
    protected Map<String, List<Double>> getPeTable() {
        return peTable;
    }

    /**
     * Sets the pe table.
     *
     * @param peTable the peTable to set
     */
    @SuppressWarnings("unchecked")
    protected void setPeTable(Map<String, ? extends List<Double>> peTable) {
        this.peTable = (Map<String, List<Double>>) peTable;
    }
}
