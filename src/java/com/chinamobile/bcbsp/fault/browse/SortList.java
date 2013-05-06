/**
 * CopyRight by Chinamobile
 * 
 * SortList.java
 */
package com.chinamobile.bcbsp.fault.browse;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *   sort the given list according the value returned by the method; and the sort 
 *    determinate the correct order  or reverted order
 *    
 */
public class SortList<E> {
    private static final Log LOG = LogFactory.getLog(SortList.class);
    
	public  void Sort(List<E> list, final String method, final String sort) {
		Collections.sort(list, new Comparator<E>() {
			@SuppressWarnings("unchecked")
			public int compare(Object a, Object b) {
				int ret = 0;
				try {
				    Object[] objs=null;
					Method m1 = ((E) a).getClass().getMethod(method, ( Class<?>[] ) objs);
					Method m2 = ((E) b).getClass().getMethod(method, ( Class<?>[] ) objs);
					if (sort != null && "desc".equals(sort))
						ret = m2.invoke(((E) b), objs).toString()
								.compareTo(m1.invoke(((E) a), objs).toString());
					else
						ret = m1.invoke(((E) a), objs).toString()
								.compareTo(m2.invoke(((E) b), objs).toString());
				} catch (NoSuchMethodException ne) {
					LOG.error("[Sort]", ne);
				} catch (IllegalAccessException ie) {
				    LOG.error("[Sort]", ie);
				} catch (InvocationTargetException it) {
				    LOG.error("[Sort]", it);
				}
				return ret;
			}
		});
	}
}

