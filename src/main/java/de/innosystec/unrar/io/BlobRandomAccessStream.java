package de.innosystec.unrar.io;

import de.innosystec.unrar.io.IReadOnlyAccess;
import java.io.EOFException;
import java.io.IOException;
import java.sql.Blob;
import java.sql.SQLException;

/**
 *
 * @author epryakhin
 */
public class BlobRandomAccessStream implements IReadOnlyAccess {

    private final Blob blob;
    private long pos;

    public BlobRandomAccessStream(Blob blob) {
        this.blob = blob;
        pos = 1;
    }

    public long getPosition() throws IOException {
        return pos;
    }

    public void setPosition(long pos) throws IOException {
        try {
            if (pos < blob.length() && pos >= 0) {
                this.pos = pos;
            } else {
                throw new EOFException();
            }
        } catch (SQLException sqle) {
            throw new IOException(sqle);
        }
    }

    /**
     * Read a single byte of data.
     */
    public int read() throws IOException {
        try {
            return blob.getBytes(pos++, 1)[0];
        } catch (SQLException sqle) {
            throw new IOException(sqle);
        }
    }

    /**
     * Read up to <tt>count</tt> bytes to the specified buffer.
     */
    public int read(byte[] buffer, int off, int count) throws IOException {
        try {
            int read = (int) Math.min(count, blob.length() - pos);
            System.arraycopy(blob.getBytes(pos, read), 0, buffer, off, read);
            pos += read;
            return read;
        } catch (SQLException sqle) {
            throw new IOException(sqle);
        }

    }

    public int readFully(byte[] buffer, int count) throws IOException {
        if (buffer == null) {
            throw new NullPointerException("buffer must not be null");
        }
        if (count == 0) {
            throw new IllegalArgumentException("cannot read 0 bytes ;-)");
        }
        try {
            int read = (int) Math.min(count, blob.length() - pos);
            System.arraycopy(blob.getBytes(pos, read), 0, buffer, 0, read);
            pos += read;
            return read;
        } catch (SQLException sqle) {
            throw new IOException(sqle);
        }

    }

    public void close() throws IOException {
    }

}
