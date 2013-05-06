/**
 * CopyRight by Chinamobile
 */
package com.chinamobile.bcbsp.test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class TestUtil {

    /**
     * This method can help trace all the places where something is printed by
     * the user.
     * 
     * @param args
     */
    public void println(Object args) {
        System.out.println(args);
    }

    /**
     * This method can help get the private attribute of object, but it needs to
     * cast the type of the result.
     * 
     * @param object
     * @param attribute
     * @return
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    public static Object get(Object object, String attribute) throws Exception {
        Class objectClass = object.getClass();
        Field field = objectClass.getDeclaredField(attribute);
        field.setAccessible(true);
        return field.get(object);
    }

    /**
     * This method can help set the private attribute of object the value.
     * 
     * @param object
     * @param attribute
     * @param value
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    public static void set(Object object, String attribute, Object value)
            throws Exception {
        Class objectClass = object.getClass();
        Field field = objectClass.getDeclaredField(attribute);
        field.setAccessible(true);
        field.set(object, value);
    }

    /**
     * This method can help invoke the private method of the object, but it needs
     * to cast the type of the result.
     * 
     * @param obj
     * @param methodName
     * @param args
     *            the arguments for the method to invoke
     * @return
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    public static Object invoke(Object obj, String methodName, Object... args)
            throws Exception {

        Class[] types = new Class[args.length];
        Class tmp = null;
        for (int i = 0; i < types.length; i++) {
            tmp = args[i].getClass();
            if (Proxy.class.isAssignableFrom(tmp)) {
                if (tmp.getInterfaces() == null
                        || tmp.getInterfaces().length == 0) {
                    if (!Proxy.class.isAssignableFrom(tmp.getSuperclass()))
                        tmp = tmp.getSuperclass();
                } else {
                    tmp = tmp.getInterfaces()[0];
                }
            }
            types[i] = tmp;
        }
        
        Method method = null;
        try{
            obj.getClass().getDeclaredMethod(methodName, types);
        } catch( NoSuchMethodException e){
            Method[] methods = obj.getClass().getDeclaredMethods();
            for(Method m: methods){
                if(m.getName().equals(methodName)){
                    Class[] pTypes = m.getParameterTypes();
                    if(pTypes.length == 0){
                        continue;
                    }
                    for(int i = 0; i < pTypes.length; i++){
                        
                    }
                }
            }
        }
        method.setAccessible(true);

        Object result = null;
        result = method.invoke(obj, args);
        return result;
    }

    public static class DonothingProxyHandler implements InvocationHandler {
        public Object invoke(Object proxy, Method method, Object[] args) {
            return null;
        }
    }

    /**
     * Create a object of a interface
     * @param theInterface
     * @return
     */
    public static <T> T createDonothingObject(Class<T> theInterface) {
        return theInterface.cast(Proxy.newProxyInstance(
                theInterface.getClassLoader(), new Class[] { theInterface },
                new DonothingProxyHandler()));
    }
}
