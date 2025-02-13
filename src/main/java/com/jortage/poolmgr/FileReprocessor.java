package com.jortage.poolmgr;

import java.io.*;
import com.jortage.poolmgr.util.PngSurgeon;
import com.jortage.poolmgr.util.PngSurgeon.CRCException;
import com.jortage.poolmgr.util.PngSurgeon.Chunk;
import com.google.common.base.Charsets;
import com.google.common.primitives.Longs;

/**
 * FileReprocessor processes input files to remove unnecessary PNG metadata.
 * It ensures better deduplication by stripping out certain chunks.
 */
public class FileReprocessor {

    /**
     * Reprocesses an input stream and writes the processed output.
     * Removes unnecessary PNG metadata that interferes with deduplication.
     *
     * @param in  InputStream of the file to process
     * @param out OutputStream where the processed data is written
     * @throws IOException if an I/O error occurs
     */
    public static void reprocess(InputStream in, OutputStream out) throws IOException {
        byte[] magicHeader = new byte[8];
        int bytesRead = in.readNBytes(magicHeader, 0, 8);
        
        if (bytesRead != 8) {
            // Not a PNG file or file is too short
            out.write(magicHeader, 0, bytesRead);
            in.transferTo(out);
            return;
        }
        
        if (Longs.fromByteArray(magicHeader) == PngSurgeon.PNG_MAGIC) {
            // PNG detected - Process using PngSurgeon
            try (var pngProcessor = new PngSurgeon(in, out)) {
                out.write(magicHeader, 0, bytesRead);
                var bufferStream = new ByteArrayOutputStream();
                byte[] buffer = new byte[512];
                
                while (true) {
                    int chunkType = pngProcessor.readChunkType();
                    
                    if (chunkType == Chunk.tIME) {
                        // Remove timestamp metadata as it affects deduplication
                        pngProcessor.skipChunkData();
                    } else if (chunkType == Chunk.tEXt) {
                        // Process tEXt chunks, filtering out unneeded metadata
                        int chunkLength = pngProcessor.getChunkLength();
                        
                        if (chunkLength < 16384) {
                            byte[] chunkData;
                            try {
                                chunkData = pngProcessor.readChunkData();
                            } catch (CRCException e) {
                                // Corrupt chunk; just copy it as-is
                                pngProcessor.copyChunk();
                                continue;
                            }
                            
                            var inputStream = new ByteArrayInputStream(chunkData);
                            bufferStream.reset();
                            
                            while (true) {
                                String key = readNullTerminatedString(inputStream, buffer, 80);
                                if (key == null) {
                                    // Corrupt tEXt chunk, write it back unchanged
                                    pngProcessor.writeChunk(Chunk.tEXt, chunkData);
                                    break;
                                } else if (key.isEmpty()) {
                                    // End of tEXt entries
                                    break;
                                }
                                
                                boolean retainChunk;
                                switch (key) {
                                    case "date:timestamp":
                                    case "date:modify":
                                    case "date:create":
                                        // Remove date-related metadata for better deduplication
                                        retainChunk = false;
                                        break;
                                    default:
                                        retainChunk = true;
                                }
                                
                                if (retainChunk) {
                                    bufferStream.write(key.getBytes(Charsets.ISO_8859_1));
                                    bufferStream.write(0);
                                    transferNullTerminatedBytes(inputStream, buffer, bufferStream);
                                    bufferStream.write(0);
                                } else {
                                    skipNullTerminatedBytes(inputStream, buffer);
                                }
                            }
                            
                            if (bufferStream.size() > 0) {
                                pngProcessor.writeChunk(Chunk.tEXt, bufferStream);
                            }
                        } else {
                            // Large chunk; copy as-is
                            pngProcessor.copyChunk();
                        }
                    } else {
                        // Copy all other chunks normally
                        pngProcessor.copyChunk();
                        if (chunkType == Chunk.IEND) break; // Stop at end of PNG file
                    }
                }
            }
        } else {
            // Not a PNG file; copy as-is
            out.write(magicHeader, 0, bytesRead);
            in.transferTo(out);
        }
    }
    
    /**
     * Reads a null-terminated string from the given input stream.
     */
    private static String readNullTerminatedString(ByteArrayInputStream is, byte[] buffer, int limit) {
        int length = readNullTerminatedBytes(is, buffer, limit);
        if (length == 0) return "";
        if (length == -1) return null;
        return new String(buffer, 0, length, Charsets.ISO_8859_1);
    }

    /**
     * Reads null-terminated bytes from a stream and returns the length.
     */
    private static int readNullTerminatedBytes(ByteArrayInputStream is, byte[] buffer, int limit) {
        is.mark(limit);
        int bytesRead = is.readNBytes(buffer, 0, limit);
        if (bytesRead == 0) return 0;
        
        int delimiterIndex = -1;
        for (int i = 0; i < bytesRead; i++) {
            if (buffer[i] == 0) {
                delimiterIndex = i;
                break;
            }
        }
        
        is.reset();
        is.skip(delimiterIndex + 1);
        return delimiterIndex;
    }
    
    /**
     * Transfers null-terminated bytes from one stream to another.
     */
    private static void transferNullTerminatedBytes(ByteArrayInputStream in, byte[] buffer, OutputStream out) throws IOException {
        while (true) {
            int length = readNullTerminatedBytes(in, buffer, buffer.length);
            if (length == 0) break;
            if (length == -1) {
                out.write(buffer);
                in.skip(buffer.length);
            } else {
                out.write(buffer, 0, length);
                break;
            }
        }
    }
    
    /**
     * Skips over null-terminated bytes in the input stream.
     */
    private static void skipNullTerminatedBytes(ByteArrayInputStream in, byte[] buffer) throws IOException {
        while (true) {
            if (readNullTerminatedBytes(in, buffer, buffer.length) != -1) break;
            in.skip(buffer.length);
        }
    }
}
