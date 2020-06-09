/*
 * Title:        CloudSim Toolkit
 * Description:  CloudSim (Cloud Simulation) Toolkit for Modeling and Simulation of Clouds
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 *
 * Copyright (c) 2009-2012, The University of Melbourne, Australia
 */

package org.cloudbus.cloudsim;

/**
 * The UtilizationModel interface needs to be implemented in order to provide a fine-grained control
 * over resource usage by a Cloudlet.
 * 
 * @author Anton Beloglazov
 * @since CloudSim Toolkit 2.0
 * @todo It has to be seen if the utilization models are only for cloudlets. If yes,
 * the name of the interface and implementing classes would include the word "Cloudlet"
 * to make clear their for what kind of entity they are related.
 */
public interface UtilizationModel {

 /**
     * Gets the <b>expected</b> utilization of resource at the current simulation time.
     * Such a value can be a percentage in scale from [0 to 1] or an absolute value,
     * depending on the {@link #getUnit()}.
     *
     * <p><b>It is an expected usage value because the actual {@link Cloudlet} resource usage
     * depends on the available {@link Vm} resource.</b></p>
     *
     * @return the current resource utilization
     * @see #getUnit()
     */
    double getUtilization(double time);


}