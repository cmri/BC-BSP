/**
 * CopyRight by Chinamobile
 */
package com.chinamobile.bcbsp.test.fault.browse;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.chinamobile.bcbsp.fault.browse.SortList;
import com.chinamobile.bcbsp.test.fault.browse.Number;

public class SortListTest {

    SortList<Number> sortList = new SortList<Number>();

    @Test
    public void testSort() {
        List<Number> list = new ArrayList<Number>();
        for (int i = 0; i < 10; i++) {
            list.add(new Number(i, String.valueOf(i)));
        }

        sortList.Sort(list, "getByInt", null);
        List<Number> listInt = new ArrayList<Number>();
        for (int i = 0; i < 10; i++) {
            listInt.add(new Number(i, String.valueOf(i)));
        }
        for (int i = 0; i < 10; i++) {
            System.out.print(listInt.get(i).getByString() + " ");
            assertEquals(listInt.get(i).getByInt(), list.get(i).getByInt());
        }

        System.out.println();

        sortList.Sort(list, "getByString", "desc");
        List<Number> listString = new ArrayList<Number>();
        for (int i = 9; i >= 0; i--) {
            listString.add(new Number(i, String.valueOf(i)));
        }
        for (int i = 0; i < 10; i++) {
            System.out.print(listString.get(i).getByString() + " ");
            assertEquals(listString.get(i).getByString(), list.get(i)
                    .getByString());
        }
    }

}
