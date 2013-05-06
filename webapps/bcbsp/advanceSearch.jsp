<%@ page contentType="text/html; charset=gb2312" language="java"
	import="java.sql.*" errorPage=""%>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
	<head>
		<meta http-equiv="Content-Type" content="text/html; charset=gb2312" />
		<title>BrowseFault</title>
	</head>

	<%!String[] types = { "WORKERNODE", "DISK", "SYSTEMSERVICE", "NETWORK" };
	String[] levels = { "INDETERMINATE", "WARNING", "MINOR", "MAJOR",
			"CRITICAL" };%>

	<body>
		<img src="chinamobile.GIF" width="69" height="62" />
		<a href="browse.jsp">FaultBrowse</a> &nbsp;&nbsp;
		<a href="search.jsp">Search</a>&nbsp;&nbsp;
		<font> AdvancedSearch</font>

		<p>
			&nbsp;
		</p>

		<%
			String userSelectType = request.getParameter("type");
			if (userSelectType == null || userSelectType.equals(""))
				userSelectType = "DISK";

			String userSelectLevel = request.getParameter("level");
			if (userSelectLevel == null || userSelectLevel.equals(""))
				userSelectLevel = "MINOR";

			String worker = request.getParameter("worker");
			if (worker == null || worker.equals("null"))
				worker = "";

			String time = request.getParameter("time");
			if (time == null || time.equals("null"))
				time = "";

			String month = request.getParameter("month");
			if (month == null)
				month = "";
		%>

		<form id="form1" name="form1" method="post" action="advanceSearch.jsp">

			<label>
				position
			</label>
			<input type="text" name="worker"
				<%if (worker != null)
				out.print("value=" + worker);%>></input>
			&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
			<label>
				type
			</label>
			<select name="type">
				<%
					for (int i = 0; i < types.length; i++) {
				%>
				<option value="<%=types[i]%>"
					<%if (userSelectType.equals(types[i]))
					out.print("selected");%>><%=types[i]%></option>
				<%
					}
				%>
			</select>
				&nbsp;&nbsp;&nbsp;&nbsp;
			
				<label>month</label>
				&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
			<input type="text" name="month" size="5"
				<%if (month != null)
				out.print("value=" + month);%>></input>
			<br />
			&nbsp;&nbsp;&nbsp;

			<label>
				time
			</label>

			&nbsp;&nbsp;
			<input type="text" name="time"
				<%if (time != null)
				out.print("value=" + time);%>></input>
			&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
			<label>
				level
			</label>
			<select name="level">
				<%
					for (int i = 0; i < levels.length; i++) {
				%>
				<option value="<%=levels[i]%>"
					<%if (userSelectLevel.equals(levels[i]))
					out.print("selected");%>><%=levels[i]%></option>
				<%
					}
				%>
			</select>
			
			&nbsp;&nbsp;&nbsp;&nbsp;
			<label>
				<input type="submit" name="Submit" value="search" />
				<input type="reset" name="reset" value="reset" />
			</label>
		</form>

		<h1>
			<jsp:useBean id="pageService" scope="page"
				class="com.chinamobile.bcbsp.fault.browse.PageService" />
			<jsp:setProperty name="pageService" property="pageNumber" />
			<jsp:setProperty name="pageService" property="type" />
			<jsp:setProperty name="pageService" property="level" />
			<jsp:setProperty name="pageService" property="time" />
			<jsp:setProperty name="pageService" property="worker" />
			<jsp:setProperty name="pageService" property="month" />
			<%!String res;%>

			<%
				if (request.getParameter("type") == null) {
					res = pageService.getFirstPage();
				} else {
					res = pageService.getPageByKeys("advanceSearch.jsp");
				}
			%>
			<%=res%>
		</h1>
	</body>

</html>
