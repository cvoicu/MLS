/**
 * Copyright 2012 EURANOVA
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */
package org.roqmessaging.scaling.launcher;

import org.roqmessaging.scaling.ScalingProcess;

/**
 * Class ScalingProcessLauncher
 * <p> Description: Launches the process of the scaling monitor.
 * 
 * @author sskhiri
 */
public class ScalingProcessLauncher {


	/**
	 * Must contain 3 attributes: 
	 * 1. The GCM IP address <br>
	 * 2. The qName <br>
	 * 3. The port on which the process will subscribe to queue configuration update<br>
	 * 
	 * example: "127.0.0.1 queueTest 5802 
	 * 
	 * <p>
	 * Notice that this process must be stopped by the shutdown monitor process
	 * by using <code>
	 *     ZMQ.Socket shutDownExChange = ZMQ.context(1).socket(ZMQ.REQ);
			shutDownExChange.setSendTimeOut(0);
			shutDownExChange.connect("tcp://"+address+":"+(listenerPort+1));
			shutDownExChange.send(Integer.toString(RoQConstant.SHUTDOWN_REQUEST).getBytes(), 0);
			shutDownExChange.close();
	 * </code>
	 * 
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws InterruptedException {
		System.out.println("Launching Scaling process with arg "+displayArg(args));
		if (args.length != 3) {
			System.out
					.println("The argument should be <GCM IP addresse> <Queue Name> < Listener port>  ");
			return;
		}
		System.out.println("Starting Scaling process for queue " + args[1] + ", using a listener port= " + args[2]);
		
		try {
			int listenerPort = Integer.parseInt(args[2]);
			// Instanciate the exchange
			final ScalingProcess scalingProcess = new ScalingProcess(args[0], args[1], listenerPort);
			scalingProcess.subscribe();
			// Launch the thread
			Thread t = new Thread(scalingProcess);
			t.start();
		} catch (NumberFormatException e) {
			System.out.println(" The arguments are not valid, must: <int: front port> <int: back port>");
		}
	}
	

	/**
	 * @param args the argument we recieved at the start up
	 * @return the concatenated string of argument
	 */
	private static String displayArg(String[] args) {
		String result="";
		for (int i = 0; i < args.length; i++) {
			result+=args[i] +", ";
		}
		return result;
	}




}
