package com.flowpowered.nbt.regionfile;

import com.flowpowered.nbt.Tag;
import com.flowpowered.nbt.stream.NBTInputStream;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RegionFileReader {


    public static List<Tag<?>> readFile(File file) {
        try {
            RegionFile file1 = new RegionFile(file);
            List<Tag<?>> tags = new ArrayList<>();
            for (int i = 0; i < 32; i++) {
                for (int j = 0; j < 32; j++) {
                    DataInputStream dis = file1.getChunkDataInputStream(i, j);
                    if(dis != null) {
                        NBTInputStream nis = new NBTInputStream(dis, false);
                        try {
                            tags.add(nis.readTag());
                        } catch (IOException e) {

                        }
                    }
                }
            }

            return tags;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
