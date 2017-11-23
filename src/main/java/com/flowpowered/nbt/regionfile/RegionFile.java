package com.flowpowered.nbt.regionfile;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.logging.Logger;
import java.util.zip.*;

public class RegionFile {
    private static final Logger logger = Logger.getLogger(RegionFile.class.getName());

    private static final byte VERSION_GZIP = 1;
    private static final byte VERSION_DEFLATE = 2;

    private static final int SECTOR_BYTES = 4096;
    private static final int SECTOR_INTS = SECTOR_BYTES / 4;

    private static final int CHUNK_HEADER_SIZE = 5;

    private static final byte[] emptySector = new byte[SECTOR_BYTES];
    private final int[] offsets;
    private final int[] chunkTimestamps;
    private RandomAccessFile file;
    private BitSet sectorsUsed;
    private int totalSectors;
    private int sizeDelta;
    private long lastModified;

    public RegionFile(File path) throws IOException {
        offsets = new int[SECTOR_INTS];
        chunkTimestamps = new int[SECTOR_INTS];

        sizeDelta = 0;

        if (path.exists()) {
            lastModified = path.lastModified();
        }

        file = new RandomAccessFile(path, "rw");

        // seek to the end to prepare size checking
        file.seek(file.length());

        // if the file size is under 8KB, grow it (4K chunk offset table, 4K timestamp table)
        if (lastModified == 0 || file.length() == 0) {
            // fast path for new or zero-length region files
            file.write(emptySector);
            file.write(emptySector);
            sizeDelta = 2 * SECTOR_BYTES;
        } else {
            if (file.length() < 2 * SECTOR_BYTES) {
                sizeDelta += 2 * SECTOR_BYTES - file.length();
                logger.warning("Region \"" + path + "\" under 8K: " + file.length() + " increasing by " + (2 * SECTOR_BYTES - file.length()));

                for (long i = file.length(); i < 2 * SECTOR_BYTES; ++i) {
                    file.write(0);
                }
            }

            // if the file size is not a multiple of 4KB, grow it
            if ((file.length() & 0xfff) != 0) {
                sizeDelta += SECTOR_BYTES - (file.length() & 0xfff);
                logger.warning("Region \"" + path + "\" not aligned: " + file.length() + " increasing by " + (SECTOR_BYTES - (file.length() & 0xfff)));

                for (long i = file.length() & 0xfff; i < SECTOR_BYTES; ++i) {
                    file.write(0);
                }
            }
        }

        // set up the available sector map
        totalSectors = (int) (file.length() / SECTOR_BYTES);
        sectorsUsed = new BitSet();

        sectorsUsed.set(0, true);
        sectorsUsed.set(1, true);

        // read offset table and timestamp tables
        file.seek(0);
        ByteBuffer header = ByteBuffer.allocate(8192);
        while (header.hasRemaining()) {
            if (file.getChannel().read(header) == -1) {
                throw new EOFException();
            }
        }
        header.clear();

        // populate the tables
        IntBuffer headerAsInts = header.asIntBuffer();
        for (int i = 0; i < SECTOR_INTS; ++i) {
            int offset = headerAsInts.get();
            offsets[i] = offset;

            int startSector = offset >> 8;
            int numSectors = offset & 0xff;

            if (offset != 0 && startSector >= 0 && startSector + numSectors <= totalSectors) {
                for (int sectorNum = 0; sectorNum < numSectors; ++sectorNum) {
                    sectorsUsed.set(startSector + sectorNum, true);
                }
            } else if (offset != 0) {
                logger.warning("Region \"" + path + "\": offsets[" + i + "] = " + offset + " -> " + startSector + "," + numSectors + " does not fit");
            }
        }
        // read timestamps from timestamp table
        for (int i = 0; i < SECTOR_INTS; ++i) {
            chunkTimestamps[i] = headerAsInts.get();
        }
    }

    /* the modification date of the region file when it was first opened */
    public long getLastModified() {
        return lastModified;
    }

    /* gets how much the region file has grown since it was last checked */
    public int getSizeDelta() {
        int ret = sizeDelta;
        sizeDelta = 0;
        return ret;
    }

    /*
     * gets an (uncompressed) stream representing the chunk data returns null if
     * the chunk is not found or an error occurs
     */
    public DataInputStream getChunkDataInputStream(int x, int z) throws IOException {
        checkBounds(x, z);

        int offset = getOffset(x, z);
        if (offset == 0) {
            // does not exist
            return null;
        }

        int sectorNumber = offset >> 8;
        int numSectors = offset & 0xFF;
        if (sectorNumber + numSectors > totalSectors) {
            throw new IOException("Invalid sector: " + sectorNumber + "+" + numSectors + " > " + totalSectors);
        }

        file.seek(sectorNumber * SECTOR_BYTES);
        int length = file.readInt();
        if (length > SECTOR_BYTES * numSectors) {
            throw new IOException("Invalid length: " + length + " > " + SECTOR_BYTES * numSectors);
        }

        byte version = file.readByte();
        if (version == VERSION_GZIP) {
            byte[] data = new byte[length - 1];
            file.read(data);
            try {
                return new DataInputStream(new BufferedInputStream(new GZIPInputStream(new ByteArrayInputStream(data), 2048)));
            } catch (ZipException e) {
                if (e.getMessage().equals("Not in GZIP format")) {
                    logger.info("Incorrect region version, switching to zlib...");
                    file.seek((sectorNumber * SECTOR_BYTES) + Integer.BYTES);
                    file.write(VERSION_DEFLATE);
                    return new DataInputStream(new BufferedInputStream(new InflaterInputStream(new ByteArrayInputStream(data), new Inflater(), 2048)));
                }
            }
        } else if (version == VERSION_DEFLATE) {
            byte[] data = new byte[length - 1];
            file.read(data);
            return new DataInputStream(new BufferedInputStream(new InflaterInputStream(new ByteArrayInputStream(data), new Inflater(), 2048)));
        }

        throw new IOException("Unknown version: " + version);
    }

    public DataOutputStream getChunkDataOutputStream(int x, int z) {
        checkBounds(x, z);
        return new DataOutputStream(new BufferedOutputStream(new DeflaterOutputStream(new ChunkBuffer(x, z), new Deflater(), 2048)));
    }

    /* write a chunk at (x,z) with length bytes of data to disk */
    protected void write(int x, int z, byte[] data, int length) throws IOException {
        int offset = getOffset(x, z);
        int sectorNumber = offset >> 8;
        int sectorsAllocated = offset & 0xFF;
        int sectorsNeeded = (length + CHUNK_HEADER_SIZE) / SECTOR_BYTES + 1;

        // maximum chunk size is 1MB
        if (sectorsNeeded >= 256) {
            return;
        }

        if (sectorNumber != 0 && sectorsAllocated == sectorsNeeded) {
            /* we can simply overwrite the old sectors */
            write(sectorNumber, data, length);
        } else {
            /* we need to allocate new sectors */

            /* mark the sectors previously used for this chunk as free */
            for (int i = 0; i < sectorsAllocated; ++i) {
                sectorsUsed.clear(sectorNumber + i);
            }

            /* scan for a free space large enough to store this chunk */
            int runStart = 2;
            int runLength = 0;
            int currentSector = 2;
            while (runLength < sectorsNeeded) {
                if (sectorsUsed.length() >= currentSector) {
                    // We reached the end, and we will need to allocate a new sector.
                    break;
                }
                int nextSector = sectorsUsed.nextClearBit(currentSector + 1);
                if (currentSector + 1 == nextSector) {
                    runLength++;
                } else {
                    runStart = nextSector;
                    runLength = 1;
                }
                currentSector = nextSector;
            }

            if (runLength >= sectorsNeeded) {
                /* we found a free space large enough */
                sectorNumber = runStart;
                setOffset(x, z, sectorNumber << 8 | sectorsNeeded);
                for (int i = 0; i < sectorsNeeded; ++i) {
                    sectorsUsed.set(sectorNumber + i);
                }
                write(sectorNumber, data, length);
            } else {
                /*
                 * no free space large enough found -- we need to grow the
                 * file
                 */
                file.seek(file.length());
                sectorNumber = totalSectors;
                for (int i = 0; i < sectorsNeeded; ++i) {
                    file.write(emptySector);
                    sectorsUsed.set(totalSectors + i);
                }
                totalSectors += sectorsNeeded;
                sizeDelta += SECTOR_BYTES * sectorsNeeded;

                write(sectorNumber, data, length);
                setOffset(x, z, sectorNumber << 8 | sectorsNeeded);
            }
        }
        setTimestamp(x, z, (int) (System.currentTimeMillis() / 1000L));
        //file.getChannel().force(true);
    }

    /* write a chunk data to the region file at specified sector number */
    private void write(int sectorNumber, byte[] data, int length) throws IOException {
        file.seek(sectorNumber * SECTOR_BYTES);
        file.writeInt(length + 1); // chunk length
        file.writeByte(VERSION_DEFLATE); // chunk version number
        file.write(data, 0, length); // chunk data
    }

    /* is this an invalid chunk coordinate? */
    private void checkBounds(int x, int z) {
        if (x < 0 || x >= 32 || z < 0 || z >= 32) {
            throw new IllegalArgumentException("Chunk out of bounds: (" + x + ", " + z + ")");
        }
    }

    private int getOffset(int x, int z) {
        return offsets[x + (z << 5)];
    }

    public boolean hasChunk(int x, int z) {
        return getOffset(x, z) != 0;
    }

    private void setOffset(int x, int z, int offset) throws IOException {
        offsets[x + (z << 5)] = offset;
        file.seek((x + (z << 5)) << 2);
        file.writeInt(offset);
    }

    private void setTimestamp(int x, int z, int value) throws IOException {
        chunkTimestamps[x + (z << 5)] = value;
        file.seek(SECTOR_BYTES + ((x + (z << 5)) << 2));
        file.writeInt(value);
    }

    public void close() throws IOException {
        file.getChannel().force(true);
        file.close();
    }

    /*
     * lets chunk writing be multithreaded by not locking the whole file as a
     * chunk is serializing -- only writes when serialization is over
     */
    class ChunkBuffer extends ByteArrayOutputStream {
        private final int x, z;

        public ChunkBuffer(int x, int z) {
            super(SECTOR_BYTES); // initialize to 4KB
            this.x = x;
            this.z = z;
        }

        @Override
        public void close() throws IOException {
            RegionFile.this.write(x, z, buf, count);
        }
    }
}
