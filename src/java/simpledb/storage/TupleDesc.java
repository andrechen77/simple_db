package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;

        /**
         * The name of the field. Is null if the field is unnamed.
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return this.fieldName + "(" + this.fieldType + ")";
        }

        public boolean equals(Object o) {
            if (!(o instanceof TDItem)) {
                return false;
            }
            TDItem other = (TDItem) o;
            return (
                (this.fieldName == null
                    ? other.fieldName == null
                    : this.fieldName.equals(other.fieldName))
            ) && this.fieldType == other.fieldType;
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return this.tdItems.iterator();
    }

    private static final long serialVersionUID = 1L;

    private ArrayList<TDItem> tdItems;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeArr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldArr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeArr, String[] fieldArr) {
        assert typeArr.length == fieldArr.length;
        this.tdItems = new ArrayList<>();
        for (int i = 0; i < typeArr.length; ++i) {
            this.tdItems.add(new TDItem(typeArr[i], fieldArr[i]));
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeArr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeArr) {
        // we interpret unnamed fields as meaning the name is null
        this.tdItems = new ArrayList<>();
        for (Type type : typeArr) {
            this.tdItems.add(new TDItem(type, null));
        }
    }

    /**
     * Constructor. Create a new tuple desc with the given TDItems.
     *
     * @param tdItems The TDItems to put in this tuple descriptor.
     */
    public TupleDesc(List<TDItem> tdItems) {
        // a shallow copy is okay because TDItems are immutable.
        this.tdItems = new ArrayList<>(tdItems);
    }

    public TupleDesc(TupleDesc old, String prefix) {
        this(
            old.tdItems
                .stream()
                .map(oldTdItem -> new TDItem(
                    oldTdItem.fieldType,
                    prefix + "." + oldTdItem.fieldName
                )).collect(Collectors.toList())
        );
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return this.tdItems.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        try {
            return this.tdItems.get(i).fieldName;
        } catch (IndexOutOfBoundsException e) {
            throw new NoSuchElementException(e);
        }
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        try {
            return this.tdItems.get(i).fieldType;
        } catch (IndexOutOfBoundsException e) {
            throw new NoSuchElementException(e);
        }
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        for (int i = 0; i < this.tdItems.size(); ++i) {
            TDItem tdItem = this.tdItems.get(i);
            if (tdItem.fieldName != null && tdItem.fieldName.equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException("No field found with matching name.");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        return this.tdItems
            .stream()
            .map(tdItem -> tdItem.fieldType.getLen())
            .reduce(0, Integer::sum);
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        List<TDItem> newTdItems = Stream.concat(
            td1.tdItems.stream(),
            td2.tdItems.stream()
        ).collect(Collectors.toList());
        return new TupleDesc(newTdItems);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        if (!(o instanceof TupleDesc)) {
            return false;
        }
        TupleDesc other = (TupleDesc) o;
        return this.tdItems.equals(other.tdItems);
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        StringBuilder result = new StringBuilder();
        for (TDItem tdItem : this.tdItems) {
            result.append(tdItem.toString());
        }
        return result.toString();
    }
}
