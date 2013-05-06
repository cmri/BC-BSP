<%@ page language="java" import="java.util.*" pageEncoding="UTF-8"%>
<%@ page import="com.chinamobile.bcbsp.bspcontroller.BSPController"%>
<%@ page import="com.chinamobile.bcbsp.workermanager.WorkerManagerStatus"%>
<%@ page import="com.chinamobile.bcbsp.util.StaffStatus"%>
<html>
  <head>  
    <title>WorkerManagerDetail.jsp</title>
	<% 
		BSPController bspController =(BSPController)application.getAttribute("bcbspController");
		Collection<WorkerManagerStatus> workerManagersStatus = bspController.groomServerStatusKeySet();
		String workerName = "slave1";
		WorkerManagerStatus thisWorkerStatus = new WorkerManagerStatus();
		WorkerManagerStatus tempWorkerStatus = new WorkerManagerStatus();
		for(Iterator<WorkerManagerStatus> iter = workerManagersStatus.iterator();iter.hasNext();){
			tempWorkerStatus = iter.next();
			if(tempWorkerStatus.getWorkerManagerName().equals(workerName)){
				thisWorkerStatus = tempWorkerStatus;
			}
		}
		session.setAttribute("StaffsStatus",thisWorkerStatus.getStaffReports());
	%>
  </head>
  
  <body>
    <h1>WorkerStation:<%=thisWorkerStatus.getWorkerManagerName() %> tasks detail</h1>
    <table border="1" width="60%" style="border-collapse: collapse">
    <tr>
    <td>maxStaffCount</td>
    <td>runningStaffCount</td>
    <td>finishStaffCount</td>
    <td>failStaffCount</td>
    </tr>
    <tr>
    <td><%=thisWorkerStatus.getMaxStaffsCount() %></td>
    <td><%=thisWorkerStatus.getRunningStaffsCount() %></td>
    <td><%=thisWorkerStatus.getFinishedStaffsCount() %></td>
    <td><%=thisWorkerStatus.getFailedStaffsCount() %></td>
    </tr>
    </table><br><hr>
    <h3>All Tasks</h3>
    <table border="1" width="60%" style="border-collapse: collapse">
    <tr>
    <td>taskID</td>
    <td>RunState</td>
    <td>Process</td>
    <td>SuperStepCount</td>
    </tr>
    <%
    int i=0;
    for(Iterator<StaffStatus> iter = ((List<StaffStatus>)session.getAttribute("StaffsStatus")).iterator();iter.hasNext();){
    	StaffStatus st = (StaffStatus)iter.next();
    	out.print("<tr><td><a href=\"StaffDetail.jsp?taskID="+st.getStaffId()+"\">"+st.getStaffId()+"</a></td>");
    	out.print("<td>"+st.getRunState()+"</td>");
    	out.print("<td>"+st.getProgress()+"</td>");
    	out.print("<td>"+st.getSuperstepCount()+"</td></tr>"); 
    	i++;
    }	
    	%>
    </table><br><hr>
    BC-BSP,2011
  </body>
</html>
