/**
 * CopyRight by Chinamobile
 * 
 * PageService.java
 */
package com.chinamobile.bcbsp.fault.browse;

import java.util.*;
import com.chinamobile.bcbsp.fault.browse.MessageService;
import com.chinamobile.bcbsp.fault.storage.Fault;

public class PageService {
    private int pageNumber = 1;
    private String type;
    private String level;
    private String time;
    private String worker;
    private String key;
    private String month;
    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        if (month == null) {
            this.month = "";
        } else if (month.equals("null")) {
            this.month = "";
        } else {
            this.month = month;
        }
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        if (time == null) {
            this.time = "";
        } else if (time.equals("null")) {
            this.time = "";
        } else {
            this.time = time;
        }
    }

    public String getWorker() {
        return worker;
    }

    public void setWorker(String worker) {
        if (worker == null) {
            this.worker = "";
        } else if (worker.equals("null")) {
            this.worker = "";
        } else {
            this.worker = worker;
        }
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        if (key == null) {
            this.key = "";
        } else if (key.equals("null")) {
            this.key = "";
        } else {
            this.key = key;
        }
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setPageNumber(int pageNumber) {
        this.pageNumber = pageNumber;
    }

    public int getPageNumber() {
        return this.pageNumber;
    }

    public String getPageByType(String path) {
        MessageService service = new MessageService();
        String monthNum = getMonth();
        List<Fault> res;
        if (monthNum == null || monthNum.equals("") || monthNum.equals("null")) {
            res = service.getPageByType(pageNumber, type);
        } else {
            int month = Integer.valueOf(monthNum);
            if (month < 0 || month > 20) {
                res = service.getPageByType(pageNumber, type);
            } else {
                res = service.getPageByType(pageNumber, type, month);
            }
        }
        StringBuffer html = new StringBuffer();
        html.append("<font size = '3' >");
        html.append("<table frame = 'box'  >");

        html.append("<tr><td width='59' bgcolor='#78A5D1'>TIME</td><td width='59' bgcolor='#78A5D1'>TYPE</td><td width='59' bgcolor='#78A5D1'>LEVEL</td><td width='59' bgcolor='#78A5D1'>WORKER</td>");
        html.append("<td width='59' bgcolor='#78A5D1'>JOBNAME</td><td width='59' bgcolor='#78A5D1'>STAFFNAME</td><td width='59' bgcolor='#78A5D1'>STATUS</td><td width='59' bgcolor='#78A5D1'>EXCEPTIONMESSAGE</td></tr>");
        if (res.size() == 0) {
            html.append("</table>");
            html.append("<label>no such records</label>");
            html.append("</font>");
            return html.toString();
        }

        for (int i = 0; i < res.size(); i++) {
            Fault fault = res.get(i);

            html.append("<tr>");
            html.append("<td>" + fault.getTimeOfFailure() + "</td>");
            html.append("<td>" + fault.getType() + "</td>");
            html.append("<td>" + fault.getLevel() + "</td>");
            html.append("<td>" + fault.getWorkerNodeName() + "</td>");
            html.append("<td>" + fault.getJobName() + "</td>");
            html.append("<td>" + fault.getStaffName() + "</td>");
            html.append("<td>" + fault.isFaultStatus() + "</td>");
            html.append("<td>" + fault.getExceptionMessage() + "</td>");
            html.append("</tr>");
        }

        int totalPages = service.getTotalPages();
        html.append("<tr>");
        if (pageNumber <= 1) {
            html.append("<td>first page</td>");
            html.append("<td>previous page</td>");
            html.append("<td><a href='" + path + "?pageNumber=" + 2 + "&type="
                    + getType() + "&month=" + getMonth()
                    + "'>next page</a></td>");
            html.append("<td><a href='" + path + "?pageNumber=" + totalPages
                    + "&type=" + getType() + "&month=" + getMonth()
                    + "'>last page</a></td>");
        } else if (pageNumber > 1 && pageNumber < totalPages) {
            html.append("<td><a href='" + path + "?pageNumber=" + 1 + "&type="
                    + getType() + "&month=" + getMonth()
                    + "'>first page</a></td>");
            html.append("<td><a href='" + path + "?pageNumber="
                    + (pageNumber - 1) + "&type=" + getType() + "&month="
                    + getMonth() + "'>previous page</a></td>");
            html.append("<td><a href='" + path + "?pageNumber="
                    + (pageNumber + 1) + "&type=" + getType() + "&month="
                    + getMonth() + "'>next page</a></td>");
            html.append("<td><a href='" + path + "?pageNumber=" + totalPages
                    + "&type=" + getType() + "&month=" + getMonth()
                    + "'>last page</a></td>");
        } else if (pageNumber == totalPages) {
            html.append("<td><a href='" + path + "?pageNumber=" + 1 + "&type="
                    + getType() + "&month=" + getMonth()
                    + "'>first page</a></td>");
            html.append("<td><a href='" + path + "?pageNumber="
                    + (pageNumber - 1) + "&type=" + getType() + "&month="
                    + getMonth() + "'>previous page</a></td>");
            html.append("<td>next page</td>");
            html.append("<td>last page</td>");
        }
        html.append("<td>currentPage " + pageNumber + "</td>");
        html.append("<td>TotalPage " + totalPages + "</td>");
        html.append("</tr>");
        html.append("</table>");
        html.append("</font>");
        return html.toString();
    }

    public String getPageBykey(String path) {
        MessageService service = new MessageService();
        String monthNum = getMonth();
        List<Fault> res;
        if (monthNum == null || monthNum.equals("") || monthNum.equals("null")) {
            res = service.getPageByKey(pageNumber, key);
        } else {
            int month = Integer.valueOf(monthNum);
            if (month < 0 || month > 20) {
                res = service.getPageByKey(pageNumber, key);
            } else {
                res = service.getPageByKey(pageNumber, key, month);
            }
        }

        StringBuffer html = new StringBuffer();
        html.append("<font size = '3' >");
        html.append("<table frame = 'box'  >");
        html.append("<tr><td width='59' bgcolor='#78A5D1'>TIME</td><td width='59' bgcolor='#78A5D1'>TYPE</td><td width='59' bgcolor='#78A5D1'>LEVEL</td><td width='59' bgcolor='#78A5D1'>WORKER</td>");
        html.append("<td width='59' bgcolor='#78A5D1'>JOBNAME</td><td width='59' bgcolor='#78A5D1'>STAFFNAME</td><td width='59' bgcolor='#78A5D1'>STATUS</td><td width='59' bgcolor='#78A5D1'>EXCEPTIONMESSAGE</td></tr>");

        if (res.size() == 0) {
            html.append("</table>");
            html.append("<label>no such records</label>");
            html.append("</font>");
            return html.toString();
        }
        for (int i = 0; i < res.size(); i++) {
            Fault fault = res.get(i);

            html.append("<tr>");
            html.append("<td>" + fault.getTimeOfFailure() + "</td>");
            html.append("<td>" + fault.getType() + "</td>");
            html.append("<td>" + fault.getLevel() + "</td>");
            html.append("<td>" + fault.getWorkerNodeName() + "</td>");
            html.append("<td>" + fault.getJobName() + "</td>");
            html.append("<td>" + fault.getStaffName() + "</td>");
            html.append("<td>" + fault.isFaultStatus() + "</td>");
            html.append("<td>" + fault.getExceptionMessage() + "</td>");
            html.append("</tr>");
        }

        int totalPages = service.getTotalPages();
        html.append("<tr>");
        if (pageNumber <= 1) {
            html.append("<td>first page</td>");
            html.append("<td>previous page</td>");
            html.append("<td><a href='" + path + "?pageNumber=" + 2 + "&key="
                    + getKey() + "&month=" + getMonth()
                    + "'>next page</a></td>");
            html.append("<td><a href='" + path + "?pageNumber=" + totalPages
                    + "&key=" + getKey() + "&month=" + getMonth()
                    + "'>last page</a></td>");
        } else if (pageNumber > 1 && pageNumber < totalPages) {
            html.append("<td><a href='" + path + "?pageNumber=" + 1 + "&key="
                    + getKey() + "&month=" + getMonth()
                    + "'>first page</a></td>");
            html.append("<td><a href='" + path + "?pageNumber="
                    + (pageNumber - 1) + "&key=" + getKey() + "&month="
                    + getMonth() + "'>previous page</a></td>");
            html.append("<td><a href='" + path + "?pageNumber="
                    + (pageNumber + 1) + "&key=" + getKey() + "&month="
                    + getMonth() + "'>next page</a></td>");
            html.append("<td><a href='" + path + "?pageNumber=" + totalPages
                    + "&key=" + getKey() + "&month=" + getMonth()
                    + "'>last page</a></td>");
        } else if (pageNumber == totalPages) {
            html.append("<td><a href='" + path + "?pageNumber=" + 1 + "&key="
                    + getKey() + "&month=" + getMonth()
                    + "'>first page</a></td>");
            html.append("<td><a href='" + path + "?pageNumber="
                    + (pageNumber - 1) + "&key=" + getKey() + "&month="
                    + getMonth() + "'>previous page</a></td>");
            html.append("<td>next page</td>");
            html.append("<td>last page</td>");
        }
        html.append("<td>currentPage " + pageNumber + "</td>");
        html.append("<td>TotalPage " + totalPages + "</td>");
        html.append("</tr>");
        html.append("</table>");
        html.append("</font>");
        return html.toString();
    }

    public String getFirstPage() {
        StringBuffer html = new StringBuffer();
        html.append("<font size = '3' >");
        html.append("<table frame = 'box'  >");
        html.append("<tr><td width='59' bgcolor='#78A5D1'>TIME</td><td width='59' bgcolor='#78A5D1'>TYPE</td><td width='59' bgcolor='#78A5D1'>LEVEL</td><td width='59' bgcolor='#78A5D1'>WORKER</td>");
        html.append("<td width='59' bgcolor='#78A5D1'>JOBNAME</td><td width='59' bgcolor='#78A5D1'>STAFFNAME</td><td width='59' bgcolor='#78A5D1'>STATUS</td><td width='59' bgcolor='#78A5D1'>EXCEPTIONMESSAGE</td></tr>");
        html.append("</table>");
        html.append("</font>");
        return html.toString();
    }

    public String getPageByKeys(String path) {
        String[] keys = { getType(), getTime(), getLevel(), getWorker() };
        MessageService service = new MessageService();
        String monthNum = getMonth();
        List<Fault> res;

        if (monthNum == null || monthNum.equals("") || monthNum.equals("null")) {
            res = service.getPageByKeys(pageNumber, keys);
        } else {
            int month = Integer.valueOf(monthNum);
            if (month < 0 || month > 20) {
                res = service.getPageByKeys(pageNumber, keys);
            } else {
                res = service.getPageByKeys(pageNumber, keys, month);
            }
        }

        StringBuffer html = new StringBuffer();
        html.append("<font size = '3' >");
        html.append("<table frame = 'box'  >");
        html.append("<tr><td width='59' bgcolor='#78A5D1'>TIME</td><td width='59' bgcolor='#78A5D1'>TYPE</td><td width='59' bgcolor='#78A5D1'>LEVEL</td><td width='59' bgcolor='#78A5D1'>WORKER</td>");
        html.append("<td width='59' bgcolor='#78A5D1'>JOBNAME</td><td width='59' bgcolor='#78A5D1'>STAFFNAME</td><td width='59' bgcolor='#78A5D1'>STATUS</td><td width='59' bgcolor='#78A5D1'>EXCEPTIONMESSAGE</td></tr>");
        if (res.size() == 0) {
            html.append("</table>");
            html.append("<label>no such records</label>");
            html.append("</font>");
            return html.toString();
        }

        for (int i = 0; i < res.size(); i++) {
            Fault fault = res.get(i);

            html.append("<tr>");
            html.append("<td>" + fault.getTimeOfFailure() + "</td>");
            html.append("<td>" + fault.getType() + "</td>");
            html.append("<td>" + fault.getLevel() + "</td>");
            html.append("<td>" + fault.getWorkerNodeName() + "</td>");
            html.append("<td>" + fault.getJobName() + "</td>");
            html.append("<td>" + fault.getStaffName() + "</td>");
            html.append("<td>" + fault.isFaultStatus() + "</td>");
            html.append("<td>" + fault.getExceptionMessage() + "</td>");
            html.append("</tr>");
        }

        int totalPages = service.getTotalPages();
        html.append("<tr>");
        if (pageNumber <= 1) {
            html.append("<td>first page</td>");
            html.append("<td>previous page</td>");
            html.append("<td><a href='" + path + "?pageNumber=" + 2 + "&type="
                    + getType() + "&level=" + getLevel() + "&time=" + getTime()
                    + "&worker=" + getWorker() + "&month=" + getMonth()
                    + "'>next page</a></td>");
            html.append("<td><a href='" + path + "?pageNumber=" + totalPages
                    + "&type=" + getType() + "&level=" + getLevel() + "&time="
                    + getTime() + "&worker=" + getWorker() + "&month="
                    + getMonth() + "'>last page</a></td>");
        } else if (pageNumber > 1 && pageNumber < totalPages) {
            html.append("<td><a href='" + path + "?pageNumber=" + 1 + "&type="
                    + getType() + "&level=" + getLevel() + "&time=" + getTime()
                    + "&worker=" + getWorker() + "&month=" + getMonth()
                    + "'>first page</a></td>");
            html.append("<td><a href='" + path + "?pageNumber="
                    + (pageNumber - 1) + "&type=" + getType() + "&level="
                    + getLevel() + "&time=" + getTime() + "&worker="
                    + getWorker() + "&month=" + getMonth()
                    + "'>previous page</a></td>");
            html.append("<td><a href='" + path + "?pageNumber="
                    + (pageNumber + 1) + "&type=" + getType() + "&level="
                    + getLevel() + "&time=" + getTime() + "&worker="
                    + getWorker() + "&month=" + getMonth()
                    + "'>next page</a></td>");
            html.append("<td><a href='" + path + "?pageNumber=" + totalPages
                    + "&type=" + getType() + "&level=" + getLevel() + "&time="
                    + getTime() + "&worker=" + getWorker() + "&month="
                    + getMonth() + "'>last page</a></td>");
        } else if (pageNumber == totalPages) {
            html.append("<td><a href='" + path + "?pageNumber=" + 1 + "&type="
                    + getType() + "&level=" + getLevel() + "&time=" + getTime()
                    + "&worker=" + getWorker() + "&month=" + getMonth()
                    + "'>first page</a></td>");
            html.append("<td><a href='" + path + "?pageNumber="
                    + (pageNumber - 1) + "&type=" + getType() + "&level="
                    + getLevel() + "&time=" + getTime() + "&worker="
                    + getWorker() + "&month=" + getMonth()
                    + "'>previous page</a></td>");
            html.append("<td>next page</td>");
            html.append("<td>last page</td>");
        }
        html.append("<td>currentPage " + pageNumber + "</td>");
        html.append("<td>TotalPage " + totalPages + "</td>");
        html.append("</tr>");
        html.append("</table>");
        html.append("</font>");
        return html.toString();
    }
}
