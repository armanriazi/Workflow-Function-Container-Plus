/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.cloudbus.cloudsim;

import static java.util.Objects.requireNonNull;

/**
 *
 * @author arman
 */
interface  ListenerCloudlet {
    
     public Cloudlet addOnUpdateProcessingListener(final EventListener<CloudletVmEventInfo> listener);

    public boolean removeOnUpdateProcessingListener(final EventListener<CloudletVmEventInfo> listener);
   
    public Cloudlet addOnStartListener(final EventListener<CloudletVmEventInfo> listener);
   
    public boolean removeOnStartListener(final EventListener<CloudletVmEventInfo> listener);
   
    public Cloudlet addOnFinishListener(final EventListener<CloudletVmEventInfo> listener) ;
    
    public boolean removeOnFinishListener(final EventListener<CloudletVmEventInfo> listener);
    
    public void notifyOnUpdateProcessingListeners(final double time);
            
}
