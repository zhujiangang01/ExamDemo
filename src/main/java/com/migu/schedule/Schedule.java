package com.migu.schedule;


import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.info.TaskInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/*
*类名和方法不能修改
 */
public class Schedule {


    private static Map<Integer, Integer> nodeMap;
    private static Map<Integer, Integer> taskMap;
    private static Map<Integer, Integer> nodeTaskMap;
    
    public int init() {
        
        nodeMap = new TreeMap<Integer, Integer>();
        taskMap = new TreeMap<Integer, Integer>();
        nodeTaskMap = new TreeMap<Integer, Integer>();
        return ReturnCodeKeys.E001;
    }


    public int registerNode(int nodeId) {
        if (0 > nodeId)
        {
            return ReturnCodeKeys.E004;
        }
        if (null != nodeMap)
        {
            if (null == nodeMap.get(nodeId))
            {
                nodeMap.put(nodeId, 0);
            }
            else
            {
                return ReturnCodeKeys.E005;
            }
            
        }
        return ReturnCodeKeys.E003;
    }

    public int unregisterNode(int nodeId) {
        if (0 > nodeId)
        {
            return ReturnCodeKeys.E004;
        }
        if (null != nodeMap)
        {
            if (null == nodeMap.get(nodeId))
            {
                return ReturnCodeKeys.E007;
            }
            else
            {
                for (Integer keyS : nodeTaskMap.keySet())
                {
                    if (nodeId == nodeTaskMap.get(keyS))
                    {
                        nodeTaskMap.put(keyS,-1);
                    }
                }
                nodeMap.remove(nodeId);
            }
            
        }
        return ReturnCodeKeys.E006;
    }


    public int addTask(int taskId, int consumption) {
        if (0 >= taskId || null==nodeMap || 0>= nodeMap.size())
        {
            return ReturnCodeKeys.E009;
        }
        if (null != taskMap)
        {
            if (null == taskMap.get(taskId))
            {
                int minC = -1;
                int minKey = 0;
                int sumC = 0;
                for (Integer keyS : nodeMap.keySet())
                {
                    if (-1 == minC)
                    {
                        minC = nodeMap.get(keyS);
                    }
                    if (minC >= nodeMap.get(keyS))
                    {
                        minC = nodeMap.get(keyS);
                        minKey = keyS;
                    }
                    sumC = sumC + nodeMap.get(keyS);
                }
                if (0 == minKey)
                {
                    return ReturnCodeKeys.E009;
                }
                {
                    taskMap.put(taskId, consumption);
                    nodeTaskMap.put(taskId, minKey);
                    nodeMap.put(minKey, consumption);
                }
            }
            else
            {
                return ReturnCodeKeys.E010;
            }
            
        }
        
        return ReturnCodeKeys.E008;
    }


    public int deleteTask(int taskId) {
        if (0 >= taskId)
        {
            return ReturnCodeKeys.E009;
        }
        if (null == taskMap.get(taskId))
        {
            return ReturnCodeKeys.E012;
        }
        else
        {
            try
            {
                int nodeId = nodeTaskMap.get(taskId);
                int sumC = nodeMap.get(nodeId);
                sumC = sumC - taskMap.get(taskId);
                taskMap.remove(taskId);
                nodeTaskMap.remove(taskId);
                nodeMap.put(nodeId, sumC);
            }
            catch (Exception e)
            {
                return ReturnCodeKeys.E009;
            }
        }
        return ReturnCodeKeys.E011;
    }


    public int scheduleTask(int threshold) {
        if (0 >= threshold)
        {
            return ReturnCodeKeys.E002;
        }
        if (null == nodeTaskMap || 0>=nodeTaskMap.size())
        {
            return ReturnCodeKeys.E013;
        }
        if (null == nodeMap || 0>=nodeMap.size())
        {
            return ReturnCodeKeys.E014;
        }
        else
        {
            List<Integer> taskIds = new ArrayList<Integer>();
            int cnt = nodeMap.size();
            
            Map<Integer, Integer> tmNodeMap = new TreeMap<Integer, Integer>();
            Map<Integer, Integer> tmTaskMap = new TreeMap<Integer, Integer>();
            int[] nodeArry = new int[nodeMap.size()];
            int[] taskArry = new int[taskMap.size()];
            if (1 == cnt)
            {
                
            }
            else
            {
                int nodeNum= 0;
                int taskNum=0;
                for (Integer nodeId : nodeMap.keySet())
                {
                    tmNodeMap.put(nodeId, 0);
                    nodeArry[nodeNum] = nodeId;
                    nodeNum++;
                    for (Integer taskId : taskMap.keySet())
                    {
                        taskArry[taskNum] = taskId;
                        taskNum++;
                    }
                }
                nodeNum= 0;
                taskNum = 0;
                int sumC = 0;
                for (Integer taskId : taskMap.keySet())
                {
                    int nodeIndex = taskNum % nodeNum - 1;
                    
                    sumC = tmNodeMap.get(nodeArry[nodeIndex]);
                    sumC = sumC + taskMap.get(taskId);
                    tmTaskMap.put(taskId, nodeArry[nodeNum]);
                    tmNodeMap.put(nodeArry[nodeNum], sumC);
                }
            }
            // nodeTaskMap.values();
            
        }
        return ReturnCodeKeys.E013;
    }


    public int queryTaskStatus(List<TaskInfo> tasks) {
        Collection<Integer> c = nodeTaskMap.keySet();
        Integer[] obj = (Integer[])c.toArray();
        Arrays.sort(obj);
        for (int i = 0; i < obj.length; i++)
        {
            TaskInfo info = new TaskInfo();
            int taskId = obj[i];
            int nodeId = nodeTaskMap.get(taskId);
            info.setNodeId(nodeId);
            info.setTaskId(taskId);
            tasks.add(info);
        }
        if (null == tasks || 0 >= tasks.size())
        {
            return ReturnCodeKeys.E016;
        }
        
        return ReturnCodeKeys.E015;
    }
    

}
