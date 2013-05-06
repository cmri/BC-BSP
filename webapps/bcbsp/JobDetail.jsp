<%@ page language="java" import="java.util.*" import="neu.bsp.test.*" pageEncoding="UTF-8"%>
<%@ page import="com.chinamobile.bcbsp.bspcontroller.BSPController"%>
<%@ page import="com.chinamobile.bcbsp.util.JobStatus"%>
<%@ page import="com.chinamobile.bcbsp.util.BSPJobID"%>
<html>
  <head>   
    <title>JobDetail</title>
    
	<%
		BSPController bspController =(BSPController)application.getAttribute("bcbspController");
		String jobID = request.getParameter("jobID");
		BSPJobID bspJobID = new BSPJobID().forName(jobID);
		JobStatus jobStatus = bspController.getJobStatus(bspJobID);
	%>

  </head>
  
  <body>
   <h1>Job Details</h1><hr>
   <table>
   <tr>
   <td><b>JobID:</b></td>
   <td><%=jobStatus.getJobID() %></td>
   </tr>
   <tr>
   <td><b>State:</b></td>
   <td><%=jobStatus.getState() %></td>
   </tr>
   <tr>
   <td><b>User:</b></td>
   <td><%=jobStatus.getUsername() %></td>
   </tr>
   <tr>
   <td><b>Progress:</b></td>
   <td><%=jobStatus.progress() %></td>
   </tr>
   <tr>
   <td><b>SetupProgress:</b></td>
   <td><%=jobStatus.setupProgress() %></td>
   </tr>
   <tr>
   <td><b>CleanupProgress</b></td>
   <td><%=jobStatus.cleanupProgress() %></td>
   </tr>
   <tr>
   <td><b>SuperStepCount:</b></td>
   <td><%=jobStatus.getSuperstepCount() %></td>
   </tr>
   <tr>
   <td><b>SchedulingInfo:</b></td>
   <td><%=jobStatus.getSchedulingInfo() %></td>
   </tr>
   <tr>
   <td><b>StartTime:</b></td>
   <td><%=new Date(jobStatus.getStartTime()).toString() %></td>
   </tr>
   <tr>
   <td><b>FinishTime:</b></td>
   <td><%=new Date(jobStatus.getFinishTime()).toString() %></td>
   </tr>
   </table>
   <hr>
   BC-BSP,2011
   
</body>
</html>