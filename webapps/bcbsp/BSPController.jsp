<%@ page language="java" import="java.util.*" pageEncoding="UTF-8"%>
<%@ page import="com.chinamobile.bcbsp.bspcontroller.BSPController"%>
<%@ page import="com.chinamobile.bcbsp.bspcontroller.ClusterStatus"%>
<%@ page import="com.chinamobile.bcbsp.util.JobStatus"%>

<html>
  <head>
    
    <title>BSPController</title>
<%	
	BSPController bspController =(BSPController)application.getAttribute("bcbspController");
	ClusterStatus clusterStatus=bspController.getClusterStatus(false);
	JobStatus[] jobs=bspController.getAllJobs();
%>
	
	
  </head>
  
  <body>
    <h1>BSPController</h1>
    <b>State:</b><%=clusterStatus.getBSPControllerState() %><hr>
    <h3>CLuster Summary</h3>
    <table border="1" width="60%" style="border-collapse: collapse">
    <tr>
    <td>Active WorkerManager</td>
    <td>currentRunningStaff</td>
    <td>Staff Capacity</td>
    </tr>
    <tr>
    <td><a href="WorkerManagersList.jsp"><%=clusterStatus.getActiveWorkerManagersCount()%></a></td>
    <td><%=clusterStatus.getRunningClusterStaffs()%></td>
    <td><%=clusterStatus.getMaxClusterStaffs()%></td>
    </tr>
    </table><br><hr> 
    <h3>Jobs Summary</h3>
    <table border="1" width="60%" style="border-collapse: collapse">
    <tr>
    <td>JobID</td>
    <td>State</td>
    <td>User</td>
    <td>Progress</td>
    <td>SuperStepCount</td>
    </tr>
    <%for(int i=0;i<jobs.length;i++){ 
			out.print("<tr><td><a href=\"JobDetail.jsp?jobID="+jobs[i].getJobID()+"\">"+jobs[i].getJobID()+"</a></td>");
			out.print("<td>"+jobs[i].getState()+"</td>");;
			out.print("<td>"+jobs[i].getUsername()+"</td>");
			out.print("<td>"+(jobs[i].progress())+"</td>");
			out.print("<td>"+(jobs[i].getSuperstepCount())+"</td></tr>");
	}%>
    </table><br><hr> 
    BC-BSP,2011
  </body>
</html>
