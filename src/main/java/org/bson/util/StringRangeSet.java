/**
 *      Copyright (C) 2008 10gen Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package org.bson.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

public class StringRangeSet implements Set<String> {

    private final int size;
    
    private final static int NUMSTR_LEN = 100;
    private final static String[] NUMSTRS = new String[100];
    static {
        for (int i = 0; i < NUMSTR_LEN; ++i)
            NUMSTRS[i] = String.valueOf(i);
    }

    public StringRangeSet(int size) {
        this.size = size;
    }

    public int size() {
        return size;
    }

    public Iterator<String> iterator() {
        return new Iterator<String>() {

            int index = 0;

            public boolean hasNext() {
                return index < size;
            }

            public String next() {
                if (index < NUMSTR_LEN)
                    return NUMSTRS[index++];
                return String.valueOf(index++);
            }
            
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

	public boolean add(String arg0) {
		throw new UnsupportedOperationException();
	}

	public boolean addAll(Collection<? extends String> arg0) {
		throw new UnsupportedOperationException();
	}

	public void clear() {
		throw new UnsupportedOperationException();
	}

	public boolean contains(Object o) {
      int t = Integer.parseInt(String.valueOf(o));
      return t >= 0 && t < size;
	}

	public boolean containsAll(Collection<?> c) {
      for (Object o : c) {
	      if (!contains(o)) {
	          return false;
	      }
      }
      return true;
	}

	public boolean isEmpty() {
		throw new UnsupportedOperationException();
	}

	public boolean remove(Object arg0) {
		throw new UnsupportedOperationException();
	}

	public boolean removeAll(Collection<?> arg0) {
		throw new UnsupportedOperationException();
	}

	public boolean retainAll(Collection<?> arg0) {
		throw new UnsupportedOperationException();
	}

	public Object[] toArray() {
      String[] array = new String[size()];
      for (int i = 0; i < size; ++i) {
          if (i < NUMSTR_LEN) {
              array[i] = NUMSTRS[i];
          } else {
              array[i] = String.valueOf(i);
          }
      }
      return array;
	}

	public <T> T[] toArray(T[] arg0) {
		throw new UnsupportedOperationException();
	}



//    @Override
//    public boolean contains(Object o) {
//        int t = Integer.parseInt(String.valueOf(o));
//        return t >= 0 && t < size;
//    }
//
//    @Override
//    public boolean containsAll(Collection<?> c) {
//        for (Object o : c) {
//            if (!contains(o)) {
//                return false;
//            }
//        }
//        return true;
//    }

//    @Override
//    public boolean isEmpty() {
//        return false;
//    }


//    @Override
//    public Object[] toArray() {
//        String[] array = new String[size()];
//        for (int i = 0; i < size; ++i) {
//            if (i < NUMSTR_LEN) {
//                array[i] = NUMSTRS[i];
//            } else {
//                array[i] = String.valueOf(i);
//            }
//        }
//        return array;
//    }

}
