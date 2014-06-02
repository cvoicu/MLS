package org.roqmessaging.log.reliability;

import java.util.ArrayList;

public class PriorityList {

	private boolean order = true;
	private ArrayList<PluginInfo> list = null;
	
	/**
	 * 
	 * @param order represent the way that elements are ordered in the list (ascendent = true or descendent = false). 
	 */
	public PriorityList(boolean order) {
		this.order = order;
		list = new ArrayList<PluginInfo>();
	}
	
	public void add(PluginInfo item) {
		if(list.size() == 0) {
			list.add(item);
		}
		else {
			int i = 0;
			while(i < list.size()) {
				if(order) {
					if(item.getCounter() <= list.get(i).getCounter()) {
						list.add(i, item);
						break;
					}
				}
				else {
					if(item.getCounter() >= list.get(i).getCounter()) {
						list.add(i, item);
						break;
					}
				}
				i++;
			}
		}
	}
	
	/**
	 * 
	 * @return the first element of the list
	 */
	public PluginInfo pick() {
		if(list.size() > 0) {
			return list.get(0);
		}
		return null;
	}
	
	/**
	 * 
	 * @param item 
	 * @return the first element of the list that is not item
	 */
	public PluginInfo pick(PluginInfo item) {
		if(list.size() > 0) {
			int i = 0; 
			while(i < list.size()) {
				if(item.getName().compareTo(list.get(i).getName()) != 0) {
					return list.get(i);
				}
				i++;
			}
		}
		return null;
	}
	
	public PluginInfo pick(ArrayList<PluginInfo> group) {
		this.list = arrangeList(list);
		if(list.size() > 0) {
			int i = 0; 
			while(i < list.size()) {
				if(group.size() > 0) {
					int j = 0;
					boolean check = true;
					while(j < group.size()) {
						if(group.get(j).getName().compareTo(list.get(i).getName()) == 0) {
							check = false;
							break;
						}
						
						j++;
					}
					if(check) {
						return list.get(i);
					}
				}
				i++;
			}
		}		
		return null;
	}
	
	public ArrayList<PluginInfo> arrangeList(ArrayList<PluginInfo> list) {
		ArrayList<PluginInfo> arrangedList = new ArrayList<PluginInfo>();
		ArrayList<PluginInfo> copyList = new ArrayList<PluginInfo>();
		int k = 0;
		while (k < list.size()) {
			copyList.add(list.get(k));
			k++;
		}
		while(copyList.size() > 0) {
			int i = 0;
			int index = 0;
			PluginInfo ni = copyList.get(0);
			while(i < copyList.size()) {
				if(ni.getCounter() > copyList.get(i).getCounter()) {
					index = i;
					ni = copyList.get(i);
				}
				i++;
			}
			arrangedList.add(ni);
			copyList.remove(index);
		}
		return arrangedList;
	}
	
	public ArrayList<PluginInfo> getArrayList() {
		return this.list;
	}
	
	public void setArrayList(ArrayList<PluginInfo> list) {
		this.list = list;
	}

}
