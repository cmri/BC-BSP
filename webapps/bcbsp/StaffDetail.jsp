<%@ page language="java" import="java.util.*" import="neu.bsp.test.*" import="neu.bsp.status.*" pageEncoding="UTF-8"%>
<%@ page import="com.chinamobile.bcbsp.util.StaffStatus"%>
<html>
  <head> 
    <title>StaffDetail.jsp</title>   
	<%
		List<StaffStatus> staffs = (List<StaffStatus>)session.getAttribute("StaffsStatus");
		String staffName = request.getParameter("taskID");
		StaffStatus thisStaffStatus = new StaffStatus();
		StaffStatus tempStaffStatus = new StaffStatus();
		for(Iterator<StaffStatus> iter=staffs.iterator();iter.hasNext();){
			tempStaffStatus = iter.next();
			if(tempStaffStatus.getStaffId().toString().equals(staffName)){
				thisStaffStatus=tempStaffStatus;
			}
		}
	%>
  </head>
  
  <body>
    <h1>Staff Details</h1><hr>
   <table>
   <tr>
   <td>TaskID:</td>
   <td><%=thisStaffStatus.getStaffId() %></td>
   </tr>
   <tr>
   <td>JobID:</td>
   <td><%=thisStaffStatus.getJobId()%></td>
   </tr>
   <tr>
   <td>State:</td>
   <td><%=thisStaffStatus.getRunState() %></td>
   </tr>
   <tr>
   <td>Phase:</td>
   <td><%=thisStaffStatus.getPhase() %></td>
   </tr>
   <tr>
   <td>Process:</td>
   <td><%=thisStaffStatus.getProgress() %></td>
   </tr>
   <tr>
   <td>GroomServer:</td>
   <td><%=thisStaffStatus.getGroomServer() %></td>
   </tr>
   <tr>
   <td>SuperStepCount:</td>
   <td><%=thisStaffStatus.getSuperstepCount() %></td>
   </tr>
   <tr>
   <td>StartTime:</td>
   <td><%=thisStaffStatus.getStartTime() %></td>
   </tr>
   <tr>
   <td>FinishTime:</td>
   <td><%=thisStaffStatus.getFinishTime() %></td>
   </tr>
   </table>
   <hr>
   BC-BSP,2011
  </body>
</html>
