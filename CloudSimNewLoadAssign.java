/*
 * Title:        CloudSim Toolkit
 * Description:  CloudSim (Cloud Simulation) Toolkit for Modeling and Simulation
 *               of Clouds
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 *
 * Copyright (c) 2009, The University of Melbourne, Australia
 */


package org.cloudbus.cloudsim.examples;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;

/**
 * An example showing how to create
 * scalable simulations.
 */
public class CloudSimNewLoadAssign {

	/** The cloudlet list. */
	private static List<Cloudlet> cloudletList;

	/** The vmlist. */
	private static List<Vm> vmlist;

	private static List<Vm> createVM(int userId, int vms) {

		//Creates a container to store VMs. This list is passed to the broker later
		LinkedList<Vm> list = new LinkedList<Vm>();

		//VM Parameters
		//long size = 10000; //image size (MB)
		int ram = 512; //vm memory (MB)
		int mips = 1000;
		long bw = 1000;
		//int pesNumber = 1; //number of cpus
		String vmm = "Xen"; //VMM name

		//create VMs
		Vm[] vm = new Vm[vms];

		Scanner sc = new Scanner(System.in);
		for(int i=0;i<vms;i++){
			System.out.println("Enter size of Server " + (i + 1) + ": ");
			long sizeVM = sc.nextLong();
			System.out.println("Enter no of active connections on Server " + (i + 1) + ": ");
			int pesNumber = sc.nextInt();
			
			vm[i] = new Vm(i, userId, mips, pesNumber, ram, bw, sizeVM, vmm, new CloudletSchedulerTimeShared());
			//for creating a VM with a space shared scheduling policy for cloudlets:
			//vm[i] = Vm(i, userId, mips, pesNumber, ram, bw, size, priority, vmm, new CloudletSchedulerSpaceShared());

			list.add(vm[i]);
		}

		return list;
	}


	private static List<Cloudlet> createCloudlet(int userId, int cloudlets, long rst){
		// Creates a container to store Cloudlets
		LinkedList<Cloudlet> list = new LinkedList<Cloudlet>();

		//cloudlet parameters
		long length = 1000;
		long fileSize = 1;
		long outputSize = rst;
		int pesNumber = 1;
		UtilizationModel utilizationModel = new UtilizationModelFull();

		Cloudlet[] cloudlet = new Cloudlet[cloudlets];

		for(int i=0;i<cloudlets;i++){
			System.out.println("Enter size of subquery " + (i+1));
			Scanner sc = new Scanner(System.in);
			fileSize = sc.nextLong();
			
			cloudlet[i] = new Cloudlet(i, length, pesNumber, fileSize, outputSize, utilizationModel, utilizationModel, utilizationModel);
			// setting the owner of these Cloudlets
			
			cloudlet[i].setUserId(userId);
			list.add(cloudlet[i]);
		}

		return list;
	}


	////////////////////////// STATIC METHODS ///////////////////////

	/**
	 * Creates main() to run this example
	 */
	public static void main(String[] args) throws IOException{
		Log.printLine("Starting Simulation...");

		try {
			// First step: Initialize the CloudSim package. It should be called
			// before creating any entities.
			int num_user = 1;   // number of grid users
			Calendar calendar = Calendar.getInstance();
			boolean trace_flag = false;  // mean trace events

			// Initialize the CloudSim library
			CloudSim.init(num_user, calendar, trace_flag);

			// Second step: Create Datacenters
			//Datacenters are the resource providers in CloudSim. We need at list one of them to run a CloudSim simulation
			@SuppressWarnings("unused")
			Datacenter datacenter0 = createDatacenter("Datacenter_0");
			//@SuppressWarnings("unused")
			//Datacenter datacenter1 = createDatacenter("Datacenter_1");
			//Third step: Create Broker
			DatacenterBroker broker = createBroker();
			int brokerId = broker.getId();

			//Fourth step: Create VMs and Cloudlets and send them to broker
			//vmlist = createVM(brokerId,20); //creating 20 vms
			System.out.println("Enter No of Servers :");
			Scanner sc = new Scanner(System.in);
			int num_VMs = sc.nextInt();
			//Fourth step: Create VMs and Cloudlets and send them to broker
			vmlist = createVM(brokerId, num_VMs); //creating 20 vms
			
			//aux -s
			System.out.println("\n\n================================================");
			System.out.println("Enter request query :");
			BufferedReader br  = new BufferedReader(new InputStreamReader(System.in));
			String s = br.readLine();
			int c = 0;
			long rst = 0;
			for(int i = 0; i < s.length(); i++) {
				if(s.charAt(i) == ',') {
					c++;
				}
			}
			if(c <= 2) {
				System.out.println("\nQuery is least loaded!");
				rst =  1;
			}
			else {
				System.out.println("\nQuery is highly loaded!");
				rst = 2;
			}
			
			//aux -e
			System.out.println("Enter request subquery size :");
			int ntasks = sc.nextInt();
			cloudletList = createCloudlet(brokerId,ntasks, rst); // creating 40 cloudlets

			broker.submitVmList(vmlist);
			broker.submitCloudletList(cloudletList);

			// Fifth step: Starts the simulation
			CloudSim.startSimulation();
			// Final step: Print results when simulation is over
			List<Cloudlet> newList = broker.getCloudletReceivedList();

			CloudSim.stopSimulation();

			printCloudletList(newList);

			Log.printLine("Done with new request Assignment!");

			vmlist = loadBalancing(vmlist);
		}
		catch (Exception e)
		{
			e.printStackTrace();
			Log.printLine("The simulation has been terminated due to an unexpected error");
		}
	}
	
	//for getting the index of min value in arr - auxx
	public static int getMin(long[] arr, int n){
	    int res = 0;
	    long min = Long.MAX_VALUE;
	    for(int i = 0; i < n; i++) {
	        if(arr[i] < min){
	                min = arr[i];
	                res = i;
	        }
	    }
	    return res;
	}

	//for getting the index of max value in arr - auxx
	public static int getMax(long[] arr, int n){
	    int res = 0;
	    long max = Long.MIN_VALUE;
	    for(int i = 0; i < n; i++) {
	        if(arr[i] >= max){
	          max = arr[i];
	          res = i;
	        }
	    }
	    return res;
	}
	
	//for visualizing server load
	public static void printServerStatus(long[] arr) {
		for(int i = 0; i< arr.length; i++) {
			long a = arr[i];
			System.out.print("Server Id : " + (i + 1) + " |  Load : ");
			for(long j = 0; j < a; j++) {
				System.out.print("=");
			}
			System.out.println(" : " + a);
		}
	}

	//created by abhijeet - aux
	private static List<Vm> loadBalancing(List<Vm> vmlist) {
		// TODO Auto-generated method stub
		int n = vmlist.size();
		long[] arr = new long[n];
		int i1 = 0;
		for(Vm v : vmlist) {
			arr[i1++] = v.getSize();
		}
		System.out.println("++++++++++++++++++++++++++++++++++++++++ LOAD BALANCING ++++++++++++++++++++++++++++++++++++++++\n\nServer status before load load balance.");
		printServerStatus(arr);
		
		//aux -s
		
		long total = 0;
		for(long i : arr) {
			total += i;
		}
		long avg = total/n; //avg of arr
		int c = 0;

		for(long i : arr) {
			if(i < avg) {
				c++;
			}
		}
		int max = 0, min = 0;
		System.out.println("\nChunkservers with load less than avg load are " + c + ".");
		for(int i = 0; i < c; i++) {
			min = getMin(arr, n);
			
			max = getMax(arr, n);

			if(min == n-1){
				arr[0] += arr[min];				
			}
			else {
				arr[min+1] += arr[min];
			}
			long k = arr[max] - avg;
			arr[max] -= k;
			if(min == n-1){
				arr[0] = (k > avg)?avg:k;
			}
			else {
				arr[min] = (k > avg)?avg:k;
			}
		}
		System.out.println("Server status before load load balance.");
		printServerStatus(arr);
		
		i1 = 0;
		for(Vm v : vmlist) {
			v.setSize(arr[i1++]);
		}
	
		
		//aux-e
		return vmlist;
	}


	private static Datacenter createDatacenter(String name){

		// Here are the steps needed to create a PowerDatacenter:
		// 1. We need to create a list to store one or more
		//    Machines
		List<Host> hostList = new ArrayList<Host>();

		// 2. A Machine contains one or more PEs or CPUs/Cores. Therefore, should
		//    create a list to store these PEs before creating
		//    a Machine.
		List<Pe> peList1 = new ArrayList<Pe>();

		int mips = 1000;

		// 3. Create PEs and add these into the list.
		//for a quad-core machine, a list of 4 PEs is required:
		peList1.add(new Pe(0, new PeProvisionerSimple(mips))); // need to store Pe id and MIPS Rating
		peList1.add(new Pe(1, new PeProvisionerSimple(mips)));
		peList1.add(new Pe(2, new PeProvisionerSimple(mips)));
		peList1.add(new Pe(3, new PeProvisionerSimple(mips)));

		//Another list, for a dual-core machine
		List<Pe> peList2 = new ArrayList<Pe>();

		peList2.add(new Pe(0, new PeProvisionerSimple(mips)));
		peList2.add(new Pe(1, new PeProvisionerSimple(mips)));

		//4. Create Hosts with its id and list of PEs and add them to the list of machines
		int hostId=0;
		int ram = 2048; //host memory (MB)
		long storage = 1000000; //host storage
		int bw = 10000;

		hostList.add(
    			new Host(
    				hostId,
    				new RamProvisionerSimple(ram),
    				new BwProvisionerSimple(bw),
    				storage,
    				peList1,
    				new VmSchedulerTimeShared(peList1)
    			)
    		); // This is our first machine

		hostId++;

		hostList.add(
    			new Host(
    				hostId,
    				new RamProvisionerSimple(ram),
    				new BwProvisionerSimple(bw),
    				storage,
    				peList2,
    				new VmSchedulerTimeShared(peList2)
    			)
    		); // Second machine


		//To create a host with a space-shared allocation policy for PEs to VMs:
		//hostList.add(
    	//		new Host(
    	//			hostId,
    	//			new CpuProvisionerSimple(peList1),
    	//			new RamProvisionerSimple(ram),
    	//			new BwProvisionerSimple(bw),
    	//			storage,
    	//			new VmSchedulerSpaceShared(peList1)
    	//		)
    	//	);

		//To create a host with a oportunistic space-shared allocation policy for PEs to VMs:
		//hostList.add(
    	//		new Host(
    	//			hostId,
    	//			new CpuProvisionerSimple(peList1),
    	//			new RamProvisionerSimple(ram),
    	//			new BwProvisionerSimple(bw),
    	//			storage,
    	//			new VmSchedulerOportunisticSpaceShared(peList1)
    	//		)
    	//	);


		// 5. Create a DatacenterCharacteristics object that stores the
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
		LinkedList<Storage> storageList = new LinkedList<Storage>();	//we are not adding SAN devices by now

		DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
                arch, os, vmm, hostList, time_zone, cost, costPerMem, costPerStorage, costPerBw);


		// 6. Finally, we need to create a PowerDatacenter object.
		Datacenter datacenter = null;
		try {
			datacenter = new Datacenter(name, characteristics, new VmAllocationPolicySimple(hostList), storageList, 0);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return datacenter;
	}

	//We strongly encourage users to develop their own broker policies, to submit vms and cloudlets according
	//to the specific rules of the simulated scenario
	private static DatacenterBroker createBroker(){

		DatacenterBroker broker = null;
		try {
			broker = new DatacenterBroker("Broker");
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return broker;
	}

	/**
	 * Prints the Cloudlet objects
	 * @param list  list of Cloudlets
	 */
	private static void printCloudletList(List<Cloudlet> list) {
		int size = list.size();
		Cloudlet cloudlet;

		String indent = "    ";
		Log.printLine();
		Log.printLine();
		Log.printLine("========== OUTPUT ==========");

		Log.printLine();
		Log.printLine("========== NEW REQUESTS STATUS ==========");
		Log.printLine("REQUEST ID" + indent + "STATUS" + indent +
				"DATA CENTER ID" + indent + "SERVER ID" + indent + indent + "Request Size" + indent + indent + "TIME" + indent + "START TIME" + indent + "FINISH TIME");

		DecimalFormat dft = new DecimalFormat("###.##");
		for (int i = 0; i < size; i++) {
			cloudlet = list.get(i);
			Log.print(indent + cloudlet.getCloudletId() + indent + indent);

			if (cloudlet.getCloudletStatus() == Cloudlet.SUCCESS){
				Log.print("SUCCESS");

				Log.printLine( indent + indent + cloudlet.getResourceId() + indent + indent + indent + indent + indent + cloudlet.getVmId() +
						indent + indent + indent + cloudlet.getCloudletFileSize() + indent + indent + indent +  dft.format(cloudlet.getActualCPUTime()) +
						indent + indent + dft.format(cloudlet.getExecStartTime())+ indent + indent + indent + dft.format(cloudlet.getFinishTime()));
			}
		}
		
		Log.printLine();
		Log.printLine("========== Virtual Machine Status ==========");
		Log.printLine("SERVER ID" + indent + " SIZE " + indent + "NO OF ACTIVE CONNECTIONS");
		
		for(Vm v : vmlist) {
			Log.printLine( indent  + v.getId() + indent + indent + indent + v.getSize() +
					indent + indent + indent + indent + v.getNumberOfPes() +
					indent + indent );
		}

	}
}
