/*
 * Cloud9: A Hadoop toolkit for working with big data
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package sourcefiles;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import tl.lin.data.array.ArrayListOfFloatsWritable;
import tl.lin.data.array.ArrayListWritable;

/**
 * Representation of a graph node for PageRank. 
 *
 * @author Jimmy Lin
 * @author Michael Schatz
 */
public class PageRankNodeEnhanced implements Writable {
  public static enum Type {
    Complete((byte) 0),  // PageRank mass and adjacency list.
    Mass((byte) 1),      // PageRank mass only.
    Structure((byte) 2); // Adjacency list only.

    public byte val;

    private Type(byte v) {
      this.val = v;
    }
  };

	private static final Type[] mapping = new Type[] { Type.Complete, Type.Mass, Type.Structure };

	private Type type;
  private String nodeid;
  private ArrayListOfFloatsWritable pageRankList;
  private ArrayListWritable<Text> adjacenyList;

  public PageRankNodeEnhanced() {
  }

  public ArrayListOfFloatsWritable getPageRankList() {
		return pageRankList;
	}

	public void setPageRankList(ArrayListOfFloatsWritable pageRankList) {
		this.pageRankList = pageRankList;
	}

  public String getNodeId() {
		return nodeid;
	}

  public void setNodeId(String n) {
		this.nodeid = n;
	}

  public ArrayListWritable<Text> getAdjacenyList() {
		return adjacenyList;
	}

  public void setAdjacencyList(ArrayListWritable<Text> list) {
		this.adjacenyList = list;
	}

	public Type getType() {
		return type;
	}

	public void setType(Type type) {
		this.type = type;
	}

	/**
	 * Deserializes this object.
	 *
	 * @param in source for raw byte representation
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		int b = in.readByte();
		type = mapping[b];
    nodeid = in.readUTF();

		if (type.equals(Type.Mass)) {
			pageRankList = new ArrayListOfFloatsWritable();
			pageRankList.readFields(in);
			return;
		}

		if (type.equals(Type.Complete)) {
			pageRankList = new ArrayListOfFloatsWritable();
			pageRankList.readFields(in);
    }

    adjacenyList = new ArrayListWritable<Text>();
    adjacenyList.readFields(in);
	}

	/**
	 * Serializes this object.
	 *
	 * @param out where to write the raw byte representation
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeByte(type.val);
    out.writeUTF(nodeid);

		if (type.equals(Type.Mass)) {
			pageRankList.write(out);
			return;
		}

		if (type.equals(Type.Complete)) {
			pageRankList.write(out);
		}

    adjacenyList.write(out);
	}

	  @Override
	  public String toString() {
    return String.format("{%s, %s, %s}", nodeid,
	    	(pageRankList == null ? "[]" : pageRankList.toString() ),
	        (adjacenyList == null ? "[]" : adjacenyList.toString()));
	  }

  /**
   * Returns the serialized representation of this object as a byte array.
   *
   * @return byte array representing the serialized representation of this object
   * @throws IOException
   */
  public byte[] serialize() throws IOException {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(bytesOut);
    write(dataOut);

    return bytesOut.toByteArray();
  }

  /**
   * Creates object from a <code>DataInput</code>.
   *
   * @param in source for reading the serialized representation
   * @return newly-created object
   * @throws IOException
   */
  public static PageRankNodeEnhanced create(DataInput in) throws IOException {
    PageRankNodeEnhanced m = new PageRankNodeEnhanced();
    m.readFields(in);

    return m;
  }

  /**
   * Creates object from a byte array.
   *
   * @param bytes raw serialized representation
   * @return newly-created object
   * @throws IOException
   */
  public static PageRankNodeEnhanced create(byte[] bytes) throws IOException {
    return create(new DataInputStream(new ByteArrayInputStream(bytes)));
  }
}
