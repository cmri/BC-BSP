/**
 * CopyRight by Chinamobile
 * 
 * ObjectSizer.java
 */
package com.chinamobile.bcbsp.util;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

import com.chinamobile.bcbsp.comm.BSPMessage;

//a reference: 4 bytes
//an Object: 8 bytes
//an Integer: 16 bytes == (8 + 4) / 8 * 8
//an int: 4 bytes
//size of array with zero elements: JRo64 = 24, Sun32 = 12
//size of reference，such as Object = null: Sun32 = 4, Sun64 = 8
//size of object without elements，such as new Object();: Sun32 = 8, Sun64 = 16
//size of byte[0]: Sun32 = 8 + 4, Sun64 = 16 + 8
//size of byte[l]: (l + 19) / 8 * 8
//size of char[l]/short[l]: (l * 2 + 19) / 8 * 8 == (l + 9) / 4 * 8
//size of String with l elements: (l + 1) * 2 + 32
//size of int[l]: (l * 4 + 19) / 8 * 8 == (l + 4) / 2 * 8
//size of long[l]: (l * 8 + 19) / 8 * 8 == (l + 2) * 8

/**
 * ObjectSizer
 * 
 * @author Bai Qiushi
 * @version 1.0
 */
public class ObjectSizer {

    private final byte NULL_REFERENCE_SIZE;
    private final byte EMPTY_OBJECT_SIZE;
    private final byte EMPTY_ARRAY_VAR_SIZE;
    @SuppressWarnings("unchecked")
    private List dedup = new ArrayList();
    
    public ObjectSizer(byte nullReferenceSize, byte emptyObjectSize,
            byte emptyArrayVarSize) {

        this.NULL_REFERENCE_SIZE = nullReferenceSize;
        this.EMPTY_OBJECT_SIZE = emptyObjectSize;
        this.EMPTY_ARRAY_VAR_SIZE = emptyArrayVarSize;

    }
    
    public static ObjectSizer forSun32BitsVM() {
        return new ObjectSizer((byte) 4, (byte) 8, (byte) 4);
    }

    public static ObjectSizer forSun64BitsVM() {
        return new ObjectSizer((byte) 8, (byte) 16, (byte) 8);
    }
    
    public int sizeOf(BSPMessage msg) {
        
        int size = this.EMPTY_OBJECT_SIZE;
        
        size = size + sizeofPrimitiveClass(int.class); // int of dstPartition
        
        size = size + 2 * msg.getDstVertexID().length() + 32; // String of dstVertex
        
        if (msg.getData() != null)
            size = size + msg.getData().length + 19; // byte[] of data
        if (msg.getTag() != null)
            size = size + msg.getTag().length + 19; // byte[] of tag
        
        return size;
    }
    
    /**
     * The size of a reference.
     * 
     * @return
     */
    public int sizeOfRef() {
        return this.NULL_REFERENCE_SIZE;
    }
    
    /**
     * The size of a char.
     * 
     * @return
     */
    public int sizeOfChar() {
        return sizeofPrimitiveClass(char.class);
    }

    /**
     * The size of an object.
     * 
     * @param object
     * @return
     */
    public int sizeOf(Object object) {
        dedup.clear();
        return calculate(object);
    }
    
    private static class ref {
        final Object obj;

        public ref(Object obj) {
            this.obj = obj;
        }

        @Override
        public boolean equals(Object obj) {
            return (obj instanceof ref) && ((ref) obj).obj == this.obj;
        }

        @Override
        public int hashCode() {
            return obj.hashCode();
        }
    }
    
    @SuppressWarnings("unchecked")
    private int calculate(Object object) {
        if (object == null)
            return 0;

        ref r = new ref(object);
        if (dedup.contains(r))
            return 0;
        dedup.add(r);

        int varSize = 0;
        int objSize = 0;
        for (Class clazz = object.getClass(); clazz != Object.class; clazz = clazz
                .getSuperclass()) {
            if (clazz.isArray()) {

                varSize += EMPTY_ARRAY_VAR_SIZE;
                Class<?> componentType = clazz.getComponentType();
                if (componentType.isPrimitive()) {
                    varSize += lengthOfPrimitiveArray(object)
                            * sizeofPrimitiveClass(componentType);
                    return OccupationSize(EMPTY_OBJECT_SIZE, varSize, 0);
                }

                Object[] array = (Object[]) object;
                varSize += NULL_REFERENCE_SIZE * array.length;
                for (Object o : array)
                    objSize += calculate(o);
                return OccupationSize(EMPTY_OBJECT_SIZE, varSize, objSize);
            }

            Field[] fields = clazz.getDeclaredFields();
            for (Field field : fields) {
                if (Modifier.isStatic(field.getModifiers()))
                    continue;

                if (clazz != field.getDeclaringClass())
                    continue;

                Class<?> type = field.getType();
                if (type.isPrimitive()) {
                    varSize += sizeofPrimitiveClass(type);
                } else {
                    varSize += NULL_REFERENCE_SIZE;
                    try {
                        field.setAccessible(true);
                        objSize += calculate(field.get(object));
                    } catch (Exception e) {
                        objSize += occupyofConstructor(object, field);
                    }
                }
            }
        }
        return OccupationSize(EMPTY_OBJECT_SIZE, varSize, objSize);
    }

    private static int occupyofConstructor(Object object, Field field) {

        throw new UnsupportedOperationException(
                "field type Constructor not accessible: " + object.getClass()
                        + " field:" + field);

    }

    private static int OccupationSize(int size) {
        return (size + 7) / 8 * 8;
    }

    private static int OccupationSize(int selfSize, int varsSize, int objsSize) {
        return OccupationSize(selfSize) + OccupationSize(varsSize) + objsSize;
    }

    @SuppressWarnings("unchecked")
    private static int sizeofPrimitiveClass(Class clazz) {
        return clazz == boolean.class || clazz == byte.class ? 1
                : clazz == char.class || clazz == short.class ? 2
                        : clazz == int.class || clazz == float.class ? 4 : 8;
    }

    private static int lengthOfPrimitiveArray(Object object) {
        Class<?> clazz = object.getClass();

        return clazz == boolean[].class ? ((boolean[]) object).length
                : clazz == byte[].class ? ((byte[]) object).length
                        : clazz == char[].class ? ((char[]) object).length
                                : clazz == short[].class ? ((short[]) object).length
                                        : clazz == int[].class ? ((int[]) object).length
                                                : clazz == float[].class ? ((float[]) object).length
                                                        : clazz == long[].class ? ((long[]) object).length
                                                                : ((double[]) object).length;

    }
}
