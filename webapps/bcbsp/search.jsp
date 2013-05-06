<%@ page contentType="text/html; charset=gb2312" language="java"
	import="java.sql.*" errorPage=""%>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
	<head>
		<meta http-equiv="Content-Type" content="text/html; charset=gb2312" />
		<title>BrowseFault</title>
	</head>
	<%!String[] types = { "TIME", "TYPE", "LEVEL", "WORKER" };%>
	<body>

		<img src="chinamobile.GIF" width="69" height="62" />
		<a href="browse.jsp">FaultBrowse</a> &nbsp;&nbsp;
		<font>Search</font>&nbsp;&nbsp;
		<a href="advanceSearch.jsp"> AdvancedSearch</a>

		<p>
			&nbsp;
		</p>
		<%
			String userKey = request.getParameter("key");
			if (userKey == null || userKey.equals("null"))
				userKey = "";

			String month = request.getParameter("month");
			if (month == null)
				month = "";
		%>


		<form id="form1" name="form1" method="post" action="search.jsp">
			&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
			<label>
				<input type="text" name="key"
					<%if (userKey != null)
				out.print("value=" + userKey);%>></input>


				<input type="submit" name="Submit" value="search" />
				<input type="reset" name="reset" value="reset" />
			</label>
			&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
			<label>
				month
			</label>
			<input type="text" name="month" size="5"
				<%if (month != null)
				out.print("value=" + month);%>></input>
		</form>

		<h1>
			<jsp:useBean id="pageService" scope="page"
				class="com.chinamobile.bcbsp.fault.browse.PageService" />
			<jsp:setProperty name="pageService" property="pageNumber" />
			<jsp:setProperty name="pageService" property="key" />
			<jsp:setProperty name="pageService" property="month" />
			<%!String res;%>

			<%
				if (request.getParameter("key") == null) {
					res = pageService.getFirstPage();
				} else {
					res = pageService.getPageBykey("search.jsp");
				}
			%>
			<%=res%>
		</h1>

	</body>

</html>
