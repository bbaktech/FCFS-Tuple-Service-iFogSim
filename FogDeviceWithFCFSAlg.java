package org.fog.test.perfeval;
import java.util.ArrayList;
import java.util.List;

import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicy;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEvent;
import org.fog.application.AppModule;
import org.fog.entities.FogDevice;
import org.fog.entities.FogDeviceCharacteristics;
import org.fog.entities.Tuple;
import org.fog.test.perfeval.FogDeviceWithFCFSAlg.ArivedTuplesListWithTimeStamp;
import org.fog.test.perfeval.FogDeviceWithFCFSAlg.ArivedTuplesListWithTimeStamp.JLEntry;
import org.fog.utils.FogEvents;
import org.fog.utils.FogUtils;
import org.fog.utils.Logger;

//FogDeviceWithFCFSAlg


public class FogDeviceWithFCFSAlg extends FogDevice {
	
	public class ArivedTuplesListWithTimeStamp {
		
		class JLEntry {
			SimEvent evt;
			double timestamp;
		};
		
		List< JLEntry > JLEntrys = new ArrayList<JLEntry>();
		
		public ArivedTuplesListWithTimeStamp(){
			
		}
		
		public double AddTuple( SimEvent ev) {
			
			JLEntry jLEntry = new JLEntry();			
			jLEntry.timestamp = CloudSim.clock();
			jLEntry.evt = ev;
			JLEntrys.add(jLEntry);		
			return jLEntry.timestamp;
		}
		
		public SimEvent GetNextTupleForExecution() {
			JLEntry jLEntry = null;
			if ( JLEntrys.size() > 0 ) {
				/*
				 * try { // Delay for 7 seonds Thread.sleep(7000); } catch(InterruptedException
				 * ex) { ex.printStackTrace(); }
				 */		
				System.out.println("No jobs in Q:"+JLEntrys.size());
				jLEntry = JLEntrys.get(0);
				JLEntrys.remove(0);
			} 	
//			System.out.println("FogDeviceWithFCFSAlg::selected Tuple For Execution");
			return jLEntry.evt;
		}
	
	};
	
	private ArivedTuplesListWithTimeStamp arivedTuplesListWithTimeStamp;
	
	public FogDeviceWithFCFSAlg(
            String name,
            FogDeviceCharacteristics characteristics,
            VmAllocationPolicy vmAllocationPolicy,
            List<Storage> storageList,
            double schedulingInterval,
            double uplinkBandwidth, double downlinkBandwidth, double uplinkLatency, double ratePerMips)  throws Exception
	{
			
		super(name, characteristics, vmAllocationPolicy, storageList,
				schedulingInterval, uplinkBandwidth, downlinkBandwidth, uplinkLatency, ratePerMips);
		arivedTuplesListWithTimeStamp = new ArivedTuplesListWithTimeStamp();
		 
	}
	
	protected void executeTuple(SimEvent ev, String moduleName)
	{
//		System.out.println("executeTuple-FogDev-"+ getHost().getId() +"-module::" + moduleName);
		 super.executeTuple(ev, moduleName);
	}

	protected void updateAllocatedMips(String incomingOperator)
	{	        
	        super.updateAllocatedMips(incomingOperator);

	}
	
	protected void checkCloudletCompletion()
	{
		super.checkCloudletCompletion();
	}
	
    protected void processTupleArrival(SimEvent event) {
    	
    	
    	arivedTuplesListWithTimeStamp.AddTuple(event);
    	
    	SimEvent ev = arivedTuplesListWithTimeStamp.GetNextTupleForExecution();
        
    	Tuple tuple = (Tuple) ev.getData();

        if (getName().equals("cloud")) {
            updateCloudTraffic();
        }
		
		/*if(getName().equals("d-0") && tuple.getTupleType().equals("_SENSOR")){
			System.out.println(++numClients);
		}*/
        Logger.debug(getName(), "Received tuple " + tuple.getCloudletId() + "with tupleType = " + tuple.getTupleType() + "\t| Source : " +
                CloudSim.getEntityName(ev.getSource()) + "|Dest : " + CloudSim.getEntityName(ev.getDestination()));
		
//		System.out.println(CloudSim.clock()+" "+getName()+" Received tuple "+tuple.getCloudletId()+" with tupleType = "+tuple.getTupleType()+"\t| Source : "+
//		CloudSim.getEntityName(ev.getSource())+"|Dest : "+CloudSim.getEntityName(ev.getDestination()));

        send(ev.getSource(), CloudSim.getMinTimeBetweenEvents(), FogEvents.TUPLE_ACK);

        if (FogUtils.appIdToGeoCoverageMap.containsKey(tuple.getAppId())) {
        }

        if (tuple.getDirection() == Tuple.ACTUATOR) {
            sendTupleToActuator(tuple);
            return;
        }

        if (getHost().getVmList().size() > 0) {
            final AppModule operator = (AppModule) getHost().getVmList().get(0);
            if (CloudSim.clock() > 0) {
            	
            	
                getHost().getVmScheduler().deallocatePesForVm(operator);
                getHost().getVmScheduler().allocatePesForVm(operator, new ArrayList<Double>() {
                    protected static final long serialVersionUID = 1L;

                    {
                        add((double) getHost().getTotalMips());
                    }
                });
            }
        }


        if (getName().equals("cloud") && tuple.getDestModuleName() == null) {
            sendNow(getControllerId(), FogEvents.TUPLE_FINISHED, null);
        }

        if (appToModulesMap.containsKey(tuple.getAppId())) {
            if (appToModulesMap.get(tuple.getAppId()).contains(tuple.getDestModuleName())) {
                int vmId = -1;
                for (Vm vm : getHost().getVmList()) {
                    if (((AppModule) vm).getName().equals(tuple.getDestModuleName()))
                        vmId = vm.getId();
                }
                if (vmId < 0
                        || (tuple.getModuleCopyMap().containsKey(tuple.getDestModuleName()) &&
                        tuple.getModuleCopyMap().get(tuple.getDestModuleName()) != vmId)) {
                    return;
                }
                tuple.setVmId(vmId);
                //Logger.error(getName(), "Executing tuple for operator " + moduleName);

                updateTimingsOnReceipt(tuple);

                executeTuple(ev, tuple.getDestModuleName());
            } else if (tuple.getDestModuleName() != null) {
                if (tuple.getDirection() == Tuple.UP)
                    sendUp(tuple);
                else if (tuple.getDirection() == Tuple.DOWN) {
                    for (int childId : getChildrenIds())
                        sendDown(tuple, childId);
                }
            } else {
                sendUp(tuple);
            }
        } else {
            if (tuple.getDirection() == Tuple.UP)
                sendUp(tuple);
            else if (tuple.getDirection() == Tuple.DOWN) {
                for (int childId : getChildrenIds())
                    sendDown(tuple, childId);
            }
        }
    }

}
