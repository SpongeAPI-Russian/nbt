package com.flowpowered.nbt;

import java.util.Arrays;

public class LongArrayTag extends Tag<long[]> {
    private final long[] value;

    public LongArrayTag(String name, long[] value) {
        super(TagType.TAG_LONG_ARRAY, name);
        this.value = value;
    }

    @Override
    public long[] getValue() {
        return value;
    }

    @Override
    public String toString() {
        StringBuilder hex = new StringBuilder();
        for (long s : value) {
            String hexDigits = Long.toHexString(s).toUpperCase();
            if (hexDigits.length() == 1) {
                hex.append("0");
            }
            hex.append(hexDigits).append(" ");
        }

        String name = getName();
        String append = "";
        if (name != null && !name.equals("")) {
            append = "(\"" + this.getName() + "\")";
        }
        return "TAG_Long_Array" + append + ": " + hex.toString();
    }

    @Override
    public Tag<long[]> clone() {
        long[] copy = Arrays.copyOf(value, value.length);
        return new LongArrayTag(getName(), copy);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        LongArrayTag that = (LongArrayTag) o;

        return Arrays.equals(value, that.value) && that.getName().equals(getName());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(value);
    }
}
