package com.jacamars.dsp.crosstalk.manager;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicReference;
 
/**
 * AtomicBigDecimal -- Mutable implementation of BigDecimal, implemented ith the concurrent atomic packag
 * @author Ben M. Faul
 *
 */

public class AtomicBigDecimal extends Number implements Comparable<Object> {
	
	/** Serialized version */
    private static final long serialVersionUID = -94825735363453200L;
    private AtomicReference<BigDecimal> value;
 

    public AtomicBigDecimal() {
        this(0.0);
    }
 
    public AtomicBigDecimal(double initVal) {
        this(new BigDecimal(initVal));
    }
     
    public AtomicBigDecimal(BigDecimal initVal) {
        value = new AtomicReference<BigDecimal>(initVal);
    }
     
    public AtomicBigDecimal(Object initVal) {
        this(objectToBigDecimal(initVal));
    }
     
    // Atomic methods
    public BigDecimal get() {
        return value.get();
    }
     
    public void set(BigDecimal newVal) {
        value.set(newVal);
    }
 
    public void set(Object newVal) {
        set(objectToBigDecimal(newVal));
    }
 
    public void set(double newVal) {
        set(new BigDecimal(newVal));
    }
     
    public void lazySet(BigDecimal newVal) {
        set(newVal);
    }
     
    public void lazySet(Object newVal) {
        set(newVal);
    }
     
    public void lazySet(double newVal) {
        set(newVal);
    }
     
    public boolean compareAndSet(BigDecimal expect, BigDecimal update) {
        while (true) {
            BigDecimal origVal = get();
 
            if (origVal.compareTo(expect) == 0) {
                if (value.compareAndSet(origVal, update))
                    return true; 
            } else {
                return false;
            }
        }
    }
     
    public boolean compareAndSet(double expect, double update) {
        return compareAndSet(new BigDecimal(expect), new BigDecimal(update));
    }
     
    public boolean compareAndSet(Object expect, Object update) {
        return compareAndSet(objectToBigDecimal(expect),
                                objectToBigDecimal(update));
    }
     
    public boolean weakCompareAndSet(BigDecimal expect, BigDecimal update) {
        return compareAndSet(expect, update);
    }
     
    public boolean weakCompareAndSet(double expect, double update) {
        return compareAndSet(expect, update);
    }
     
    public boolean weakCompareAndSet(Object expect, Object update) {
        return compareAndSet(expect, update);
    }
     
    public BigDecimal getAndSet(BigDecimal setVal) {
        while (true) {
            BigDecimal origVal = get();
 
            if (compareAndSet(origVal, setVal)) return origVal;
        }
    }
     
    public double getAndSet(double setVal) {
        return getAndSet(new BigDecimal(setVal)).doubleValue();
    }
     
    public BigDecimal getAndSet(Object setVal) {
        return getAndSet(objectToBigDecimal(setVal));
    }
     
    public BigDecimal getAndAdd(BigDecimal delta) {
        while (true) {
            BigDecimal origVal = get();
            BigDecimal newVal = origVal.add(delta);
            if (compareAndSet(origVal, newVal)) return origVal;
        }
    }
 
    public BigDecimal addAndGet(BigDecimal delta) {
        while (true) {
            BigDecimal origVal = get();
            BigDecimal newVal = origVal.add(delta);
            if (compareAndSet(origVal, newVal)) return newVal;
        }
    }
     
    public double getAndAdd(double delta) {
        return getAndAdd(new BigDecimal(delta)).doubleValue();
    }
     
    public double addAndGet(double delta) {
        return addAndGet(new BigDecimal(delta)).doubleValue();
    }
     
    public BigDecimal getAndAdd(Object delta) {
        return getAndAdd(objectToBigDecimal(delta));
    }
 
    public BigDecimal addAndGet(Object delta) {
        return addAndGet(objectToBigDecimal(delta));
    }
     
    public BigDecimal getAndIncrement() {
        return getAndAdd(BigDecimal.ONE);
    }
 
    public BigDecimal getAndDecrement() {
        return getAndAdd(BigDecimal.ONE.negate());
    }
 
    public BigDecimal incrementAndGet() {
        return addAndGet(BigDecimal.ONE);
    }
 
    public BigDecimal decrementAndGet() {
        return addAndGet(BigDecimal.ONE.negate());
    }
     
    public double altGetAndIncrement() {
        return getAndIncrement().doubleValue();
    }
 
    public double altGetAndDecrement() {
        return getAndDecrement().doubleValue();
    }
 
    public double altIncrementAndGet() {
        return incrementAndGet().doubleValue();
    }
 
    public double altDecrementAndGet() {
        return decrementAndGet().doubleValue();
    }
 
    public BigDecimal getAndMultiply(BigDecimal multiplicand) {
        while (true) {
            BigDecimal origVal = get();
            BigDecimal newVal = origVal.multiply(multiplicand);
            if (compareAndSet(origVal, newVal)) return origVal;
        }
    }
 
    public BigDecimal multiplyAndGet(BigDecimal multiplicand) {
        while (true) {
            BigDecimal origVal = get();
            BigDecimal newVal = origVal.multiply(multiplicand);
            if (compareAndSet(origVal, newVal)) return newVal;
        }
    }
    public BigDecimal getAndDivide(BigDecimal divisor) {
        while (true) {
            BigDecimal origVal = get();
            BigDecimal newVal = origVal.divide(divisor);
            if (compareAndSet(origVal, newVal)) return origVal;
        }
    }
 
    public BigDecimal divideAndGet(BigDecimal divisor) {
        while (true) {
            BigDecimal origVal = get();
            BigDecimal newVal = origVal.divide(divisor);
            if (compareAndSet(origVal, newVal)) return newVal;
        }
    }
     
    public BigDecimal getAndMultiply(Object multiplicand) {
        return getAndMultiply(objectToBigDecimal(multiplicand));
    }
 
    public BigDecimal multiplyAndGet(Object multiplicand) {
        return multiplyAndGet(objectToBigDecimal(multiplicand));
    }
     
    public BigDecimal getAndDivide(Object divisor) {
        return getAndDivide(objectToBigDecimal(divisor));
    }
 
    public BigDecimal divideAndGet(Object divisor) {
        return divideAndGet(objectToBigDecimal(divisor));
    }
     
    // Methods of the Number class
    @Override
    public int intValue() {
        return getBigDecimalValue().intValue();
    }
     
    @Override
    public long longValue() {
        return getBigDecimalValue().longValue();
    }
     
    @Override
    public float floatValue() {
        return getBigDecimalValue().floatValue();
    }
     
    @Override
    public double doubleValue() {
        return getBigDecimalValue().doubleValue();
    }
     
    @Override
    public byte byteValue() {
        return (byte)intValue();
    }
     
    @Override
    public short shortValue() {
        return (short)intValue();
    }
     
    public char charValue() {
        return (char)intValue();
    }
 
    public BigDecimal getBigDecimalValue() {
        return get();
    }
     
    public Double getDoubleValue() {
        return Double.valueOf(doubleValue());
    }
 
    public boolean isNaN() {
        return getDoubleValue().isNaN();
    }
 
    public boolean isInfinite() {
        return getDoubleValue().isInfinite();
    }
  
    // Methods of the BigDecimal Class
    public BigDecimal abs() {
        while (true) {
            BigDecimal origVal = get();
            BigDecimal newVal = origVal.abs();
            if (compareAndSet(origVal, newVal)) return newVal;
        }
    }
     
    public BigDecimal max(BigDecimal val) {
        while (true) {
            BigDecimal origVal = get();
            BigDecimal newVal = origVal.max(val);
            if (compareAndSet(origVal, newVal)) return newVal;
        }
    }
     
    public BigDecimal min(BigDecimal val) {
        while (true) {
            BigDecimal origVal = get();
            BigDecimal newVal = origVal.min(val);
            if (compareAndSet(origVal, newVal)) return newVal;
        }
    }
     
    public BigDecimal negate() {
        while (true) {
            BigDecimal origVal = get();
            BigDecimal newVal = origVal.negate();
            if (compareAndSet(origVal, newVal)) return newVal;
        }
    }
     
    public BigDecimal pow(int n) {
        while (true) {
            BigDecimal origVal = get();
            BigDecimal newVal = origVal.pow(n);
            if (compareAndSet(origVal, newVal)) return newVal;
        }
    }
     
    // Support methods for hashing and comparing
    @Override
    public String toString() {
        return get().toString();
    }
     
    public String toEngineeringString() {
        return get().toEngineeringString();
    }
     
    public String toPlainString() {
        return get().toPlainString();
    }
     
    @Override
    public boolean equals(Object obj) {
        if (obj == null) return false;
        
        if (!(obj instanceof AtomicBigDecimal))
            return false;

        try {
            return (compareTo(obj) == 0);
        } catch (Exception e) {
            return false;
        }
    }
     
    @Override
    public int hashCode() {
        return get().hashCode();
    }
     
    public int compareTo(AtomicBigDecimal aValue) {
        return get().compareTo(aValue.get());
    }
     
    public int compareTo(Object aValue) {
        return get().compareTo(objectToBigDecimal(aValue));
    }
     
    public int compareTo(double aValue) {
        return get().compareTo(new BigDecimal(aValue));
    }
     
    public static AtomicDoubleComparator comparator =
                            new AtomicDoubleComparator();
    public static class AtomicDoubleComparator 
                            implements Comparator<Object> {
        public int compare(AtomicBigDecimal d1, AtomicBigDecimal d2) {
            return d1.compareTo(d2);
        }
         
        public int compare(Object d1, Object d2) {
            return objectToBigDecimal(d1).compareTo(objectToBigDecimal(d2));
        }
         
        public int compareReverse(AtomicBigDecimal d1, AtomicBigDecimal d2) {
            return d2.compareTo(d1);
        }
         
        public int compareReverse(Object d1, Object d2) {
            return objectToBigDecimal(d2).compareTo(objectToBigDecimal(d1));
        }
    }
     
    // Support Routines and constants
    private static BigDecimal objectToBigDecimal(Object obj) {
        if (obj instanceof AtomicBigDecimal) {
            return ((AtomicBigDecimal) obj).get();
        } if (obj instanceof BigDecimal) {
            return (BigDecimal) obj;
        } else if (obj instanceof BigInteger) {
            return new BigDecimal((BigInteger)obj);
        } else if (obj instanceof String) {
            return new BigDecimal((String) obj);
        } else if (obj instanceof Number) {
            return new BigDecimal(((Number)obj).doubleValue());
        } else {
            return new BigDecimal(obj.toString());
        }
    }
}
